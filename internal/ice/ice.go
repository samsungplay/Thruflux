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
	// This is blocking but fast. We can make it async if needed, but usually we need it before sharing candidates.
	if err := p.resolvePublicAddr(); err != nil {
		logger.Warn("failed to resolve public address (STUN)", "error", err)
		// Non-fatal, we might still work on LAN
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

// Close closes the underlying UDP connection.
func (p *Prober) Close() error {
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
			if iface.Flags&net.FlagUp == 0 {
				continue
			}
			if iface.Flags&net.FlagLoopback != 0 {
				continue // Skip loopback usually
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
				if ip == nil {
					continue
				}
				if ip.To4() == nil {
					// Skip IPv6 for now if you want to keep it simple, or include it.
					// Let's include it but maybe prioritize IPv4.
					// For simplicity in this refactor, let's stick to IPv4 to avoid complexity
					// unless we want full dual-stack.
					continue
				}

				// Construct candidate string: "ip:port"
				// Note: We use the PORT bound by our socket (p.udpConn.LocalAddr().Port)
				// NOT the port of the interface (which doesn't exist).
				// But wait, if we bind to 0.0.0.0:Port, we can be reached at <InterfaceIP>:Port.
				_, portStr, _ := net.SplitHostPort(p.udpConn.LocalAddr().String())
				candidates = append(candidates, net.JoinHostPort(ip.String(), portStr))
			}
		}
	}

	// 2. Public IP (WAN)
	if p.publicAddr != nil {
		candidates = append(candidates, p.publicAddr.String())
	}

	return candidates
}

// ProbeAndDial concurrently dials the given list of remote addresses using QUIC.
// It returns the first successfully established connection.
func (p *Prober) ProbeAndDial(ctx context.Context, remoteCandidates []string, tlsConf any, quicConf *quic.Config) (*quic.Conn, error) {
	// We need to assert tlsConf is *tls.Config. Since we can't import crypto/tls here easily
	// without adding it to imports (which is fine), but allow caller to pass it.
	// Actually, we should import crypto/tls. But to keep signature clean, let's assume caller passes compatible config.
	// However, quic.Dial requires *tls.Config.

	// Let's rely on the caller to pass correct types or standard interface.
	// For now, we will assume the caller imports quic-go, so we expose `quic.Dial`.
	// But `quic.Dial` takes `net.PacketConn`.

	// Helper to parse address
	parseAddr := func(addrStr string) (net.Addr, error) {
		return net.ResolveUDPAddr("udp", addrStr)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan *quic.Conn, 1) // Buffer 1 is enough for the winner
	errCh := make(chan error, len(remoteCandidates))

	var wg sync.WaitGroup

	// We use the SAME PacketConn for all dials.
	// quic-go supports this: Dialing multiple times from the same PacketConn.
	// It internally manages the demuxing of 1-RTT packets based on Source Connection ID.

	dialCandidate := func(addrStr string) {
		defer wg.Done()

		udpAddr, err := parseAddr(addrStr)
		if err != nil {
			p.logger.Warn("invalid remote candidate", "addr", addrStr, "error", err)
			return
		}

		p.logger.Debug("probing candidate", "addr", addrStr)

		// Create a separate context for this dial so we can timeout individual attempts if needed,
		// but generally we just rely on the main ctx or QUIC handshake timeout.
		// We use `quic.Dial` which creates a session.

		// To allow passing tlsConfig cleanly, we might need to modify imports.
		// For now, let's assume `tlsConf` is passed as `*tls.Config` and cast it.
		// If we can't cast, we panic or error.
		// Ideally we import "crypto/tls". Let's do that.

		// Note: We use Dial (not DialAddr) because we want to use our specific PacketConn.

		// Wait... quic.Dial takes `context.Context, net.PacketConn, net.Addr, *tls.Config, *quic.Config`
		// We need to fix the imports to carry `crypto/tls`.
		conn, err := quic.Dial(ctx, p.udpConn, udpAddr, tlsConf.(*tls.Config), quicConf)
		if err != nil {
			// This returns error if handshake fails.
			// Common error: timeout, or connection refused (ICMP).
			p.logger.Debug("probe failed", "addr", addrStr, "error", err)
			errCh <- err
			return
		}

		// Success!
		select {
		case resultCh <- conn:
			p.logger.Info("probe won", "addr", addrStr)
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

	// Wait for one success or all failures
	// We can't really wait for "all done" easily while also returning early.
	// But we can use a goroutine to close errCh when wg is done.
	go func() {
		wg.Wait()
		close(errCh)
	}()

	select {
	case conn := <-resultCh:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-func() chan struct{} {
		// Wait for all to fail
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		return done
	}():
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
