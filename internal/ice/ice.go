package ice

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strconv"
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
	publicAddrs []net.Addr
	mu         sync.Mutex
}

// NewProber creates a new network prober.
// It opens a UDP socket for listening and probing.
func NewProber(cfg ProberConfig, logger *slog.Logger) (*Prober, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	// Open a single UDP socket for everything.
	// Prefer dual-stack to allow IPv4+IPv6 candidates.
	udpAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve local address: %w", err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		// Fallback to IPv4-only if dual-stack isn't available.
		udpAddr, err = net.ResolveUDPAddr("udp4", ":0")
		if err != nil {
			return nil, fmt.Errorf("failed to resolve local address: %w", err)
		}
		conn, err = net.ListenUDP("udp4", udpAddr)
	}
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

// PublicAddr returns one public address discovered via STUN, or nil if failed.
func (p *Prober) PublicAddr() net.Addr {
	if len(p.publicAddrs) == 0 {
		return nil
	}
	return p.publicAddrs[0]
}

// Listen returns the underlying UDP connection to be used for QUIC listening.
func (p *Prober) ListenPacket() net.PacketConn {
	return p.udpConn
}

// Close closes the underlying UDP connection or transport.
func (p *Prober) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.transport != nil {
		return p.transport.Close()
	}
	return p.udpConn.Close()
}

// Transport returns the underlying quic.Transport, initializing it if needed.
// This allows callers to use the same transport for both dialing and listening.
func (p *Prober) Transport() *quic.Transport {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.transport == nil {
		p.transport = &quic.Transport{
			Conn: p.udpConn,
		}
	}
	return p.transport
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

				if ip == nil || ip.IsMulticast() || ip.IsUnspecified() {
					continue
				}

				// Allow both IPv4 and IPv6
				// ip.String() handles IPv6 format (e.g. ::1) correctly.
				// net.JoinHostPort handles wrapping IPv6 in brackets [::1]:port.
				host := ip.String()
				if ip.IsLinkLocalUnicast() {
					// Link-local IPv6 needs a zone (interface name) to be dialable.
					host = (&net.IPAddr{IP: ip, Zone: iface.Name}).String()
				}
				cand := net.JoinHostPort(host, portStr)
				candidates = append(candidates, cand)
			}
		}
	}

	// 2. Public IPs (WAN)
	if len(p.publicAddrs) > 0 {
		for _, addr := range p.publicAddrs {
			candidates = append(candidates, addr.String())
		}
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
func (p *Prober) ProbeAndDial(ctx context.Context, remoteCandidates []string, tlsConf any, quicConf *quic.Config, onUpdate func(ProbeUpdate)) (*quic.Conn, error) {
	// Initialize Transport if not already done.
	// We do this here (lazy init) or we could do it earlier, but STUN works better on raw UDP.
	p.mu.Lock()
	if p.transport == nil {
		p.transport = &quic.Transport{
			Conn: p.udpConn,
		}
	}
	p.mu.Unlock()

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

	var resolved bool
	seen := make(map[string]struct{})
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
		serverAddrs, err := resolveStunAddrs(addrStr)
		if err != nil {
			p.logger.Warn("invalid STUN server", "server", server, "error", err)
			continue
		}

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

		for _, serverAddr := range serverAddrs {
			p.logger.Debug("sending STUN request", "server", serverAddr.String())

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

			var mapped *net.UDPAddr
			var xorAddr stun.XORMappedAddress
			if err := xorAddr.GetFrom(res); err != nil {
				// Try MappedAddress
				var mappedAddr stun.MappedAddress
				if err := mappedAddr.GetFrom(res); err != nil {
					continue
				}
				mapped = &net.UDPAddr{IP: mappedAddr.IP, Port: mappedAddr.Port}
			} else {
				mapped = &net.UDPAddr{IP: xorAddr.IP, Port: xorAddr.Port}
			}

			if mapped != nil {
				key := mapped.String()
				if _, ok := seen[key]; !ok {
					seen[key] = struct{}{}
					p.publicAddrs = append(p.publicAddrs, mapped)
					p.logger.Info("public address resolved", "addr", mapped)
					resolved = true
				}
			}
		}
	}

	if !resolved {
		return fmt.Errorf("all STUN servers failed")
	}
	return nil
}

func resolveStunAddrs(addrStr string) ([]*net.UDPAddr, error) {
	host, portStr, err := net.SplitHostPort(addrStr)
	if err != nil {
		addr, err := net.ResolveUDPAddr("udp", addrStr)
		if err != nil {
			return nil, err
		}
		return []*net.UDPAddr{addr}, nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupIPAddr(context.Background(), host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no IPs for %s", host)
	}
	addrs := make([]*net.UDPAddr, 0, len(ips))
	for _, ip := range ips {
		addrs = append(addrs, &net.UDPAddr{IP: ip.IP, Port: port})
	}
	return addrs, nil
}
