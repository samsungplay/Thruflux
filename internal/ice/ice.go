package ice

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pion/stun"
	"github.com/quic-go/quic-go"
)

// ProberConfig holds configuration for the network prober.
type ProberConfig struct {
	StunServers []string
	PreferLAN   bool
}

// DefaultStunServers is the STUN list used when no servers provided.
var DefaultStunServers = []string{
	"stun.l.google.com:19302",
	"stun.cloudflare.com:3478",
}

// Prober manages network discovery and probing.
type Prober struct {
	config     ProberConfig
	logger     *slog.Logger
	udpConn    *net.UDPConn
	transport  *quic.Transport
	publicAddr net.Addr
}

// NewProber creates a new network prober.
// It opens a UDP socket for listening and probing.
func NewProber(cfg ProberConfig, logger *slog.Logger) (*Prober, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	// Open a single UDP socket for everything
	// We bind to 0.0.0.0:0 to let the OS pick a port and listen on all interfaces
	udpAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve local address: %w", err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on UDP: %w", err)
	}

	p := &Prober{
		config:  cfg,
		logger:  logger,
		udpConn: conn,
	}

	// Resolve public address via STUN
	if err := p.resolvePublicAddr(); err != nil {
		logger.Warn("failed to resolve public address (STUN)", "error", err)
	}

	return p, nil
}

// LocalAddr returns the local address of the underlying UDP socket.
func (p *Prober) LocalAddr() net.Addr {
	return p.udpConn.LocalAddr()
}

// PublicAddr returns the public address discovered via STUN, or nil if failed.
func (p *Prober) PublicAddr() net.Addr {
	return p.publicAddr
}

// Listen returns the underlying UDP connection to be used for QUIC listening.
func (p *Prober) ListenPacket() net.PacketConn {
	return p.udpConn
}

// Close closes the underlying UDP connection or transport.
func (p *Prober) Close() error {
	if p.transport != nil {
		return p.transport.Close()
	}
	return p.udpConn.Close()
}

// GetProbingAddresses returns a list of local and public addresses to share with peers.
func (p *Prober) GetProbingAddresses() []string {
	var candidates []string

	// 1. Local Interface IPs (LAN)
	ifaces, err := net.Interfaces()
	if err != nil {
		p.logger.Error("failed to list interfaces", "error", err)
	} else {
		for _, iface := range ifaces {
			// Skip down interfaces
			if iface.Flags&net.FlagUp == 0 {
				continue
			}
			// Skip loopback unless we really want it (usually we don't for LAN transfer)
			if iface.Flags&net.FlagLoopback != 0 {
				continue
			}

			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}

			_, portStr, _ := net.SplitHostPort(p.udpConn.LocalAddr().String())

			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}

				if ip == nil || ip.IsLoopback() || ip.IsMulticast() {
					continue
				}

				// Allow both IPv4 and IPv6
				// ip.String() handles IPv6 format (e.g. ::1) correctly.
				// net.JoinHostPort handles wrapping IPv6 in brackets [::1]:port.
				cand := net.JoinHostPort(ip.String(), portStr)
				candidates = append(candidates, cand)
			}
		}
	}

	// 2. Public IP (WAN)
	if p.publicAddr != nil {
		candidates = append(candidates, p.publicAddr.String())
	}

	// Log gathered
	p.logger.Info("gathered probing candidates", "count", len(candidates), "candidates", candidates)

	return candidates
}

// ProbeState represents the state of an individual address probe.
type ProbeState int

const (
	ProbeStateProbing ProbeState = iota
	ProbeStateFailed
	ProbeStateWon
)

func (s ProbeState) String() string {
	switch s {
	case ProbeStateProbing:
		return "probing"
	case ProbeStateFailed:
		return "failed"
	case ProbeStateWon:
		return "won"
	default:
		return "unknown"
	}
}

// ProbeUpdate represents a status update for a single address probe.
type ProbeUpdate struct {
	Addr  string
	State ProbeState
	Err   error
}

// ProbeAndDial concurrently dials the given list of remote addresses using QUIC.
// It returns the first successfully established connection.
// The onUpdate callback is called whenever a probe's state changes.
func (p *Prober) ProbeAndDial(ctx context.Context, remoteCandidates []string, tlsConf any, quicConf *quic.Config, onUpdate func(ProbeUpdate)) (*quic.Conn, error) {
	// Initialize Transport if not already done.
	// We do this here (lazy init) or we could do it earlier, but STUN works better on raw UDP.
	if p.transport == nil {
		p.transport = &quic.Transport{
			Conn: p.udpConn,
		}
	}

	// Helper to parse address
	parseAddr := func(addrStr string) (net.Addr, error) {
		return net.ResolveUDPAddr("udp", addrStr)
	}

	// We use a child context for dialing to cancel losers
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan *quic.Conn, 1)

	// Track active attempts
	var wg sync.WaitGroup

	dialCandidate := func(addrStr string) {
		defer wg.Done()

		if onUpdate != nil {
			onUpdate(ProbeUpdate{Addr: addrStr, State: ProbeStateProbing})
		}

		udpAddr, err := parseAddr(addrStr)
		if err != nil {
			p.logger.Warn("invalid remote candidate", "addr", addrStr, "error", err)
			if onUpdate != nil {
				onUpdate(ProbeUpdate{Addr: addrStr, State: ProbeStateFailed, Err: err})
			}
			return
		}

		p.logger.Debug("probing candidate", "addr", addrStr)

		// Use Transport.Dial
		conn, err := p.transport.Dial(ctx, udpAddr, tlsConf.(*tls.Config), quicConf)
		if err != nil {
			p.logger.Debug("probe failed", "addr", addrStr, "error", err)
			if onUpdate != nil {
				onUpdate(ProbeUpdate{Addr: addrStr, State: ProbeStateFailed, Err: err})
			}
			return
		}

		// Success!
		select {
		case resultCh <- conn:
			p.logger.Info("probe won", "addr", addrStr)
			if onUpdate != nil {
				onUpdate(ProbeUpdate{Addr: addrStr, State: ProbeStateWon})
			}
		default:
			// Lost the race, close this connection
			conn.CloseWithError(0, "race_lost")
		}
	}

	uniqueCandidates := make(map[string]bool)
	for _, c := range remoteCandidates {
		uniqueCandidates[c] = true
	}

	for c := range uniqueCandidates {
		wg.Add(1)
		go dialCandidate(c)
	}

	// Wait for all to finish in a separate goroutine to detect failure
	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()

	select {
	case conn := <-resultCh:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-allDone:
		return nil, fmt.Errorf("all probes failed")
	}
}

func (p *Prober) resolvePublicAddr() error {
	// Simple STUN client
	// servers := p.config.StunServers
	servers := DefaultStunServers
	if len(p.config.StunServers) > 0 {
		servers = p.config.StunServers
	}

	for _, server := range servers {
		// We can't use our main p.udpConn for "pion/stun" easily because
		// pion/stun Client expects to own the connection or at least read from it exclusively
		// during the transaction.
		// Since we haven't started QUIC yet, we CAN use p.udpConn!
		// But we need to be careful not to discard packets if we were multithreaded.
		// Here we are in NewProber, so it's safe.

		// Parse STUN server address
		// server format "host:port" or "stun:host:port"
		addrStr := strings.TrimPrefix(server, "stun:")
		serverAddr, err := net.ResolveUDPAddr("udp", addrStr)
		if err != nil {
			p.logger.Warn("invalid STUN server", "server", server, "error", err)
			continue
		}

		p.logger.Debug("sending STUN request", "server", addrStr)

		// Create a STUN client
		// We use `stun.Dial` usually, but we want to use OUR connection.
		// pion/stun/v3 might allow `Client` with existing conn.
		// Let's double check imports. The user has `pion/stun/v2` or `v3`?
		// go.mod said `github.com/pion/stun/v3 v3.1.1 // indirect`.
		// But `github.com/pion/ice/v2` brings old dependencies?
		// go.mod has `github.com/pion/ice/v2`.
		// Let's use `pion/stun` package.

		// Send a binding request
		msg := stun.MustBuild(stun.TransactionID, stun.BindingRequest)

		// Write to server
		if _, err := p.udpConn.WriteToUDP(msg.Raw, serverAddr); err != nil {
			continue
		}

		// Wait for response with timeout
		buf := make([]byte, 1024)
		p.udpConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, _, err := p.udpConn.ReadFromUDP(buf)
		p.udpConn.SetReadDeadline(time.Time{}) // Reset
		if err != nil {
			continue
		}

		res := &stun.Message{Raw: buf[:n]}
		if err := res.Decode(); err != nil {
			continue
		}

		var xorAddr stun.XORMappedAddress
		if err := xorAddr.GetFrom(res); err != nil {
			// Try MappedAddress
			var mappedAddr stun.MappedAddress
			if err := mappedAddr.GetFrom(res); err != nil {
				continue
			}
			p.publicAddr = &net.UDPAddr{IP: mappedAddr.IP, Port: mappedAddr.Port}
		} else {
			p.publicAddr = &net.UDPAddr{IP: xorAddr.IP, Port: xorAddr.Port}
		}

		p.logger.Info("public address resolved", "addr", p.publicAddr)
		return nil
	}

	return fmt.Errorf("all STUN servers failed")
}
