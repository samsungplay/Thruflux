package ice

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/pion/ice/v2"
)

// ICEConfig holds configuration for ICE peer.
type ICEConfig struct {
	StunServers []string
	Lite        bool // false for now
}

// ICEPeer manages ICE connection establishment.
type ICEPeer struct {
	agent            *ice.Agent
	config           ICEConfig
	logger           *slog.Logger
	mu               sync.Mutex
	onCandidate      func(string)
	queuedCandidates []string // Candidates gathered before callback is set
	// Connection info for QUIC
	conn             *ice.Conn
	localAddr        net.Addr
	remoteAddr       net.Addr
}

// NewICEPeer creates a new ICE peer with the given configuration.
func NewICEPeer(cfg ICEConfig, logger *slog.Logger) (*ICEPeer, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	// Default STUN server if none provided
	stunServers := cfg.StunServers
	if len(stunServers) == 0 {
		stunServers = []string{"stun:stun.l.google.com:19302"}
	}

	// Convert to pion format
	var urls []*ice.URL
	for _, server := range stunServers {
		url, err := ice.ParseURL(server)
		if err != nil {
			return nil, fmt.Errorf("invalid STUN server URL %s: %w", server, err)
		}
		urls = append(urls, url)
	}

	config := &ice.AgentConfig{
		NetworkTypes: []ice.NetworkType{ice.NetworkTypeUDP4,
		ice.NetworkTypeUDP6},
		Urls:         urls,
	}

	agent, err := ice.NewAgent(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create ICE agent: %w", err)
	}

	peer := &ICEPeer{
		agent:  agent,
		config: cfg,
		logger: logger,
	}

	// Set up candidate callback
	if err := agent.OnCandidate(func(candidate ice.Candidate) {
		if candidate == nil {
			return
		}
		candidateStr := candidate.Marshal()
		peer.logger.Debug("local candidate gathered", "candidate", candidateStr)
		peer.mu.Lock()
		onCandidate := peer.onCandidate
		if onCandidate == nil {
			// Queue candidate if callback not yet set
			peer.queuedCandidates = append(peer.queuedCandidates, candidateStr)
			peer.mu.Unlock()
			peer.logger.Debug("queued candidate (callback not set yet)", "candidate", candidateStr)
			return
		}
		peer.mu.Unlock()
		onCandidate(candidateStr)
	}); err != nil {
		agent.Close()
		return nil, fmt.Errorf("failed to set candidate callback: %w", err)
	}

	return peer, nil
}

// StartGathering starts gathering ICE candidates.
func (p *ICEPeer) StartGathering(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.agent.GatherCandidates(); err != nil {
		return fmt.Errorf("failed to gather candidates: %w", err)
	}

	return nil
}

// LocalCredentials returns the local ICE credentials (username fragment and password).
func (p *ICEPeer) LocalCredentials() (ufrag, pwd string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ufrag, pwd, _ = p.agent.GetLocalUserCredentials()
	return ufrag, pwd
}

// AddRemoteCredentials sets the remote ICE credentials.
func (p *ICEPeer) AddRemoteCredentials(ufrag, pwd string) error {
	if ufrag == "" || pwd == "" {
		return fmt.Errorf("credentials cannot be empty: ufrag=%q pwd=%q", ufrag, pwd)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.agent.SetRemoteCredentials(ufrag, pwd); err != nil {
		return fmt.Errorf("failed to set remote credentials: %w", err)
	}
	// Verify they were set
	verifyUfrag, verifyPwd, err := p.agent.GetRemoteUserCredentials()
	if err != nil {
		p.logger.Warn("failed to verify remote credentials after setting", "error", err)
	} else if verifyUfrag != ufrag || verifyPwd != pwd {
		p.logger.Warn("remote credentials mismatch after setting",
			"expected_ufrag", ufrag, "got_ufrag", verifyUfrag,
			"expected_pwd", pwd, "got_pwd", verifyPwd)
	} else {
		p.logger.Debug("remote credentials set and verified", "ufrag", ufrag)
	}
	return nil
}

// OnLocalCandidate sets a callback function that is called when a local candidate is gathered.
// Any candidates gathered before this callback is set will be flushed immediately.
func (p *ICEPeer) OnLocalCandidate(fn func(c string)) {
	p.mu.Lock()
	p.onCandidate = fn
	// Flush any queued candidates
	queued := p.queuedCandidates
	p.queuedCandidates = nil
	p.mu.Unlock()

	// Call callback for queued candidates (outside lock)
	for _, c := range queued {
		p.logger.Debug("flushing queued candidate", "candidate", c)
		fn(c)
	}
}

// AddRemoteCandidate adds a remote ICE candidate.
func (p *ICEPeer) AddRemoteCandidate(c string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	candidate, err := ice.UnmarshalCandidate(c)
	if err != nil {
		return fmt.Errorf("failed to unmarshal candidate: %w", err)
	}

	if err := p.agent.AddRemoteCandidate(candidate); err != nil {
		return fmt.Errorf("failed to add remote candidate: %w", err)
	}

	p.logger.Debug("remote candidate added", "candidate", c)
	return nil
}

// Connect establishes a connection as the controlling agent (caller/sender).
// This should be called after credentials and candidates have been exchanged.
func (p *ICEPeer) Connect(ctx context.Context) (net.Conn, error) {
	return p.connect(ctx, true)
}

// Accept establishes a connection as the controlled agent (callee/receiver).
// This should be called after credentials and candidates have been exchanged.
func (p *ICEPeer) Accept(ctx context.Context) (net.Conn, error) {
	return p.connect(ctx, false)
}

func (p *ICEPeer) connect(ctx context.Context, isControlling bool) (net.Conn, error) {
	// Verify remote credentials are set (with lock)
	p.mu.Lock()
	remoteUfrag, remotePwd, err := p.agent.GetRemoteUserCredentials()
	if err != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("failed to get remote credentials: %w", err)
	}
	if remoteUfrag == "" || remotePwd == "" {
		p.mu.Unlock()
		return nil, fmt.Errorf("remote credentials not set: ufrag=%q pwd=%q", remoteUfrag, remotePwd)
	}
	
	// Also verify local credentials
	localUfrag, localPwd, _ := p.agent.GetLocalUserCredentials()
	if localUfrag == "" || localPwd == "" {
		p.mu.Unlock()
		return nil, fmt.Errorf("local credentials not available: ufrag=%q pwd=%q", localUfrag, localPwd)
	}
	p.mu.Unlock()

	p.logger.Debug("connecting with credentials", "local_ufrag", localUfrag, "remote_ufrag", remoteUfrag, "controlling", isControlling)
	
	var conn *ice.Conn
	if isControlling {
		// Sender calls Dial (controlling agent)
		conn, err = p.agent.Dial(ctx, remoteUfrag, remotePwd)
	} else {
		// Receiver calls Accept (controlled agent)
		conn, err = p.agent.Accept(ctx, remoteUfrag, remotePwd)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to establish ICE connection: %w", err)
	}

	// Get the selected candidate pair for logging and storing addresses
	selectedPair, err := p.agent.GetSelectedCandidatePair()
	if err == nil && selectedPair != nil {
		p.logger.Info("ICE connection established",
			"local", selectedPair.Local.String(),
			"remote", selectedPair.Remote.String())
		
		// Get addresses from the actual connection (more reliable than candidate)
		connLocalAddr := conn.LocalAddr()
		connRemoteAddr := conn.RemoteAddr()
		
		// Parse remote address from candidate (connection RemoteAddr might not be UDP)
		remoteIP := selectedPair.Remote.Address()
		remotePort := selectedPair.Remote.Port()
		remoteAddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(remoteIP, fmt.Sprintf("%d", remotePort)))
		if err != nil {
			p.logger.Warn("failed to parse remote address", "ip", remoteIP, "port", remotePort, "error", err)
			// Fallback to connection's remote addr if available
			if connRemoteAddr != nil {
				if udpAddr, ok := connRemoteAddr.(*net.UDPAddr); ok {
					remoteAddr = udpAddr
				}
			}
		}
		
		// Store connection info for QUIC
		p.mu.Lock()
		p.conn = conn
		// Use connection's local address (actual bound address)
		if connLocalAddr != nil {
			if udpAddr, ok := connLocalAddr.(*net.UDPAddr); ok {
				p.localAddr = udpAddr
			} else {
				// If not UDP, try to parse from candidate
				localIP := selectedPair.Local.Address()
				localPort := selectedPair.Local.Port()
				if localUDPAddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(localIP, fmt.Sprintf("%d", localPort))); err == nil {
					p.localAddr = localUDPAddr
				}
			}
		}
		if remoteAddr != nil {
			p.remoteAddr = remoteAddr
		}
		p.mu.Unlock()
	}

	return conn, nil
}

// PacketConnInfo returns the local and remote addresses for QUIC transport.
// This should be called after Connect() or Accept() succeeds.
func (p *ICEPeer) PacketConnInfo() (localAddr, remoteAddr net.Addr, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.localAddr == nil || p.remoteAddr == nil {
		return nil, nil, fmt.Errorf("ICE connection not established yet")
	}
	
	return p.localAddr, p.remoteAddr, nil
}

// CreatePacketConn creates a new UDP PacketConn for QUIC.
// It attempts to bind to the same local address as ICE. If that fails (because ICE
// is still using it), it will close the ICE connection first, then bind to that address.
// This ensures QUIC uses the same network path as ICE.
func (p *ICEPeer) CreatePacketConn() (net.PacketConn, error) {
	localAddr, _, err := p.PacketConnInfo()
	if err != nil {
		return nil, err
	}
	
	// Parse the address
	udpAddr, ok := localAddr.(*net.UDPAddr)
	if !ok {
		return nil, fmt.Errorf("local address is not a UDP address: %T", localAddr)
	}
	
	// Try to bind to the same address first
	conn, err := net.ListenPacket("udp", udpAddr.String())
	if err != nil {
		// If binding fails, close ICE connection first to free up the address
		p.logger.Debug("could not bind to ICE local address, closing ICE connection first", "error", err)
		p.mu.Lock()
		if p.conn != nil {
			p.conn.Close()
			p.conn = nil
		}
		p.mu.Unlock()
		
		// Now try again
		conn, err = net.ListenPacket("udp", udpAddr.String())
		if err != nil {
			return nil, fmt.Errorf("failed to create UDP PacketConn after closing ICE: %w", err)
		}
	}
	
	p.logger.Debug("created PacketConn for QUIC", "local_addr", conn.LocalAddr())
	return conn, nil
}

// Close closes the ICE agent and cleans up resources.
func (p *ICEPeer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.agent.Close()
}

