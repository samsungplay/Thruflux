package ice

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pion/ice/v2"
	"github.com/pion/transport/v2/stdnet"
)

// ICEConfig holds configuration for ICE peer.
type ICEConfig struct {
	StunServers []string
	TurnServers []string
	Lite        bool // false for now
	PreferLAN   bool
}

// DefaultStunServers is the STUN list used when no servers provided.
var DefaultStunServers = []string{
	"stun:stun.l.google.com:19302",
	"stun:stun.cloudflare.com:3478",
	"stun:stun.bytepipe.app:3478",
}

// ICEPeer manages ICE connection establishment.
type ICEPeer struct {
	agent            *ice.Agent
	config           ICEConfig
	logger           *slog.Logger
	mu               sync.Mutex
	onCandidate      func(string)
	queuedCandidates []string // Candidates gathered before callback is set
	gatherDone       chan struct{}
	gatherOnce       sync.Once
	// Connection info for QUIC
	conn         *ice.Conn
	localAddr    net.Addr
	remoteAddr   net.Addr
	selectedPair *ice.CandidatePair
	udpConns     []*net.UDPConn
	udpMux       ice.UDPMux
	demuxers     map[ice.NetworkType]*packetDemux
}

// NewICEPeer creates a new ICE peer with the given configuration.
func NewICEPeer(cfg ICEConfig, logger *slog.Logger) (*ICEPeer, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	// Default STUN server if none provided
	servers := append([]string{}, cfg.StunServers...)
	servers = append(servers, cfg.TurnServers...)
	if len(servers) == 0 {
		servers = append([]string{}, DefaultStunServers...)
	}

	// Convert to pion format
	var urls []*ice.URL
	for _, server := range servers {
		url, err := ice.ParseURL(server)
		if err != nil {
			return nil, fmt.Errorf("invalid ICE server URL %s: %w", server, err)
		}
		urls = append(urls, url)
	}

	netTypes, udpConns, udpMux, demuxers, err := createUDPMux()
	if err != nil {
		return nil, err
	}

	config := &ice.AgentConfig{
		NetworkTypes: netTypes,
		Urls:         urls,
		UDPMux:       udpMux,
	}

	if cfg.PreferLAN {
		config.InterfaceFilter = func(name string) bool {
			iface, err := net.InterfaceByName(name)
			if err != nil {
				return false // Can't score it, so skip it to be safe in strict mode
			}
			// Score >= 50 means it's likely a physical or standard interface
			return CalculateSpeedScore(*iface) >= 50
		}
		config.IPFilter = func(ip net.IP) bool {
			// Block loopback addresses (127.0.0.1, ::1)
			// These paths often have poor throughput performance in pion
			if ip.IsLoopback() {
				return false
			}
			return true
		}
	}

	agent, err := ice.NewAgent(config)
	if err != nil {
		if udpMux != nil {
			_ = udpMux.Close()
		}
		for _, conn := range udpConns {
			_ = conn.Close()
		}
		return nil, fmt.Errorf("failed to create ICE agent: %w", err)
	}

	peer := &ICEPeer{
		agent:      agent,
		config:     cfg,
		logger:     logger,
		gatherDone: make(chan struct{}),
		udpConns:   udpConns,
		udpMux:     udpMux,
		demuxers:   demuxers,
	}

	// Set up candidate callback
	if err := agent.OnCandidate(func(candidate ice.Candidate) {
		if candidate == nil {
			peer.gatherOnce.Do(func() { close(peer.gatherDone) })
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

func createUDPMux() ([]ice.NetworkType, []*net.UDPConn, ice.UDPMux, map[ice.NetworkType]*packetDemux, error) {
	netInstance, err := stdnet.NewNet()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to create network: %w", err)
	}

	var (
		netTypes []ice.NetworkType
		conns    []*net.UDPConn
		demuxes  = make(map[ice.NetworkType]*packetDemux)
	)

	conn4, err4 := netInstance.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err4 == nil {
		udp4, ok := conn4.(*net.UDPConn)
		if !ok {
			_ = conn4.Close()
			err4 = fmt.Errorf("unexpected udp4 conn type %T", conn4)
		} else {
			netTypes = append(netTypes, ice.NetworkTypeUDP4)
			conns = append(conns, udp4)
			demuxes[ice.NetworkTypeUDP4] = newPacketDemux(udp4)
		}
	}

	conn6, err6 := netInstance.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6zero, Port: 0})
	if err6 == nil {
		udp6, ok := conn6.(*net.UDPConn)
		if !ok {
			_ = conn6.Close()
			err6 = fmt.Errorf("unexpected udp6 conn type %T", conn6)
		} else {
			netTypes = append(netTypes, ice.NetworkTypeUDP6)
			conns = append(conns, udp6)
			demuxes[ice.NetworkTypeUDP6] = newPacketDemux(udp6)
		}
	}

	if len(demuxes) == 0 {
		return nil, nil, nil, nil, fmt.Errorf("failed to create UDP sockets: udp4=%v udp6=%v", err4, err6)
	}

	udpMux, err := newDemuxUDPMux(demuxes)
	if err != nil {
		for _, demux := range demuxes {
			_ = demux.Close()
		}
		return nil, nil, nil, nil, err
	}

	return netTypes, conns, udpMux, demuxes, nil
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

// GatheringDone returns a channel that is closed when candidate gathering completes.
func (p *ICEPeer) GatheringDone() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gatherDone
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
		p.logger.Debug("ICE connection established",
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
		p.selectedPair = selectedPair
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

	if p.conn == nil {
		return nil, nil, fmt.Errorf("ICE connection not established yet")
	}

	return p.localAddr, p.remoteAddr, nil
}

// UDPConns returns the UDP sockets used for ICE candidate gathering.
func (p *ICEPeer) UDPConns() []*net.UDPConn {
	p.mu.Lock()
	conns := append([]*net.UDPConn(nil), p.udpConns...)
	p.mu.Unlock()
	return conns
}

// CreatePacketConn returns a PacketConn for QUIC data using the ICE-established
// UDP socket, then hands off control by stopping ICE.
func (p *ICEPeer) CreatePacketConn() (net.PacketConn, error) {
	p.mu.Lock()
	if p.conn == nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("ICE connection not established")
	}
	demux, err := p.demuxForSelectedPairLocked()
	agent := p.agent
	p.agent = nil
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}

	if agent != nil {
		_ = agent.Close()
	}
	if demux == nil {
		return nil, fmt.Errorf("no UDP demuxer for handoff")
	}
	demux.Stop()
	conn := demux.Conn()
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func (p *ICEPeer) demuxForSelectedPairLocked() (*packetDemux, error) {
	if len(p.demuxers) == 0 {
		return nil, fmt.Errorf("no UDP demuxers available")
	}
	if p.selectedPair != nil {
		if demux, ok := p.demuxers[p.selectedPair.Local.NetworkType()]; ok {
			return demux, nil
		}
	}
	if p.remoteAddr != nil {
		if udpAddr, ok := p.remoteAddr.(*net.UDPAddr); ok {
			if udpAddr.IP.To4() == nil {
				if demux, ok := p.demuxers[ice.NetworkTypeUDP6]; ok {
					return demux, nil
				}
			} else {
				if demux, ok := p.demuxers[ice.NetworkTypeUDP4]; ok {
					return demux, nil
				}
			}
		}
	}
	for _, demux := range p.demuxers {
		return demux, nil
	}
	return nil, fmt.Errorf("no UDP demuxer for selected ICE pair")
}

// SelectedCandidatePair returns the chosen ICE candidate pair, if available.

func (p *ICEPeer) SelectedCandidatePair() *ice.CandidatePair {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.selectedPair
}

// IsRemoteCandidateSrflxOrRelay returns true if the selected remote candidate is srflx or relay.
func (p *ICEPeer) IsRemoteCandidateSrflxOrRelay() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.selectedPair == nil {
		return false
	}
	typ := p.selectedPair.Remote.Type()
	return typ == ice.CandidateTypeServerReflexive || typ == ice.CandidateTypePeerReflexive || typ == ice.CandidateTypeRelay
}

func (p *ICEPeer) Close() error {
	p.mu.Lock()
	agent := p.agent
	udpMux := p.udpMux
	demuxers := p.demuxers
	p.agent = nil
	p.udpMux = nil
	p.udpConns = nil
	p.demuxers = nil
	p.mu.Unlock()

	if udpMux != nil {
		_ = udpMux.Close()
	}
	if agent != nil {
		_ = agent.Close()
	}
	for _, demux := range demuxers {
		if demux != nil {
			_ = demux.Close()
		}
	}
	return nil
}

// CalculateSpeedScore assigns a rating (0-100) to a network interface.
func CalculateSpeedScore(iface net.Interface) int {
	score := 50 // Start neutral

	// 1. FATAL CHECKS
	if iface.Flags&net.FlagUp == 0 {
		return 0
	}
	if iface.Flags&net.FlagLoopback != 0 {
		return 5
	}

	name := strings.ToLower(iface.Name)

	// 2. NAME HEURISTICS
	physicalPrefixes := []string{"en", "eth", "wlan", "wifi"}
	for _, p := range physicalPrefixes {
		if strings.HasPrefix(name, p) {
			score += 30
			break
		}
	}

	virtualPrefixes := []string{
		"utun", "tun", "tap", "feth", "zt", "wg", "docker", "vbox", "ppp",
	}
	for _, p := range virtualPrefixes {
		if strings.HasPrefix(name, p) {
			score -= 40
			break
		}
	}

	// 3. FLAG PHYSICS
	if iface.Flags&net.FlagPointToPoint != 0 {
		score -= 20
	}
	if iface.Flags&net.FlagBroadcast != 0 {
		score += 10
	}
	if iface.Flags&net.FlagMulticast != 0 {
		score += 5
	}

	// 4. MTU REALITY CHECK
	switch {
	case iface.MTU > 1500:
		score += 10
	case iface.MTU == 1500:
		score += 0
	case iface.MTU < 1280:
		score -= 15
	case iface.MTU < 1500:
		score -= 5
	}

	// Clamp score
	if score > 100 {
		return 100
	}
	if score < 0 {
		return 0
	}

	return score
}
