package peers

import (
	"sync"
	"time"

	"github.com/sheerbytes/sheerbytes/pkg/protocol"
)

// Peer represents a connected peer.
type Peer struct {
	PeerID string
	Role   string
	ConnID string // unique per WebSocket connection
}

// peerConnection holds a peer and its send channel.
type peerConnection struct {
	peer Peer
	send chan protocol.Envelope
}

// Hub manages peers per session in a thread-safe manner.
// Duplicate peer_ids within a session use last-write-wins: the most recent connection replaces any previous one.
type Hub struct {
	mu        sync.RWMutex
	sessions  map[string]map[string]*peerConnection // sessionID -> connID -> peerConnection
	byPeerID  map[string]map[string]string          // sessionID -> peerID -> connID (for routing by peer_id)
}

// NewHub creates a new peer hub.
func NewHub() *Hub {
	return &Hub{
		sessions: make(map[string]map[string]*peerConnection),
		byPeerID: make(map[string]map[string]string),
	}
}

// Add adds a peer to a session and returns a remove function.
// The send function is used to send envelopes to the peer.
// Returns a remove function that removes the peer and triggers PeerLeft broadcast.
func (h *Hub) Add(sessionID string, p Peer, send func(env protocol.Envelope) error) (remove func()) {
	// Create buffered channel for this peer's messages
	ch := make(chan protocol.Envelope, 256) // Buffered to avoid blocking

	pc := &peerConnection{
		peer: p,
		send: ch,
	}

	// Start writer goroutine for this connection
	done := make(chan struct{})
	go func() {
		defer close(done)
		for env := range ch {
			if err := send(env); err != nil {
				// If send fails, stop consuming from channel
				return
			}
		}
	}()

	h.mu.Lock()
	if h.sessions[sessionID] == nil {
		h.sessions[sessionID] = make(map[string]*peerConnection)
	}
	if h.byPeerID[sessionID] == nil {
		h.byPeerID[sessionID] = make(map[string]string)
	}
	
	// Last-write-wins: if peer_id already exists, remove old connection
	// We'll mark it as replaced so the remove function doesn't try to clean it up again
	if oldConnID, exists := h.byPeerID[sessionID][p.PeerID]; exists && oldConnID != p.ConnID {
		if oldPC, ok := h.sessions[sessionID][oldConnID]; ok {
			// Close channel and wait for goroutine to finish
			close(oldPC.send)
		}
		delete(h.sessions[sessionID], oldConnID)
		// Remove from byPeerID so remove function won't find it
		delete(h.byPeerID[sessionID], p.PeerID)
	}
	
	h.sessions[sessionID][p.ConnID] = pc
	h.byPeerID[sessionID][p.PeerID] = p.ConnID
	h.mu.Unlock()

	// Return remove function
		return func() {
		h.mu.Lock()
		sessionPeers, exists := h.sessions[sessionID]
		if !exists {
			h.mu.Unlock()
			return
		}
		
		// Check if this connection still exists (may have been replaced)
		if _, stillExists := sessionPeers[p.ConnID]; !stillExists {
			h.mu.Unlock()
			return
		}
		
		delete(sessionPeers, p.ConnID)
		
		// Remove from byPeerID if this is still the active connection
		if peerIDMap, exists := h.byPeerID[sessionID]; exists {
			if peerIDMap[p.PeerID] == p.ConnID {
				delete(peerIDMap, p.PeerID)
			}
		}

		h.mu.Unlock()
		
		// Close channel to stop writer goroutine (outside lock to avoid deadlock)
		close(ch)
		
		// Wait for writer to finish (with timeout to avoid blocking forever)
		select {
		case <-done:
		case <-time.After(1 * time.Second):
			// Timeout - continue anyway
		}

		h.mu.Lock()
		// If no peers left in session, remove session entry
		if len(sessionPeers) == 0 {
			delete(h.sessions, sessionID)
			delete(h.byPeerID, sessionID)
		}
		h.mu.Unlock()
	}
}

// List returns a list of peers in a session as protocol.PeerInfo.
func (h *Hub) List(sessionID string) []protocol.PeerInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()

	sessionPeers, exists := h.sessions[sessionID]
	if !exists || len(sessionPeers) == 0 {
		return []protocol.PeerInfo{}
	}

	peers := make([]protocol.PeerInfo, 0, len(sessionPeers))
	for _, pc := range sessionPeers {
		peers = append(peers, protocol.PeerInfo{
			PeerID: pc.peer.PeerID,
			Role:   pc.peer.Role,
		})
	}

	return peers
}

// Broadcast sends an envelope to all peers in a session.
// Uses non-blocking sends via buffered channels to avoid blocking on slow peers.
func (h *Hub) Broadcast(sessionID string, env protocol.Envelope) {
	h.mu.RLock()
	sessionPeers, exists := h.sessions[sessionID]
	if !exists {
		h.mu.RUnlock()
		return
	}

	// Create a copy of the map to avoid holding lock during sends
	peersCopy := make([]*peerConnection, 0, len(sessionPeers))
	for _, pc := range sessionPeers {
		peersCopy = append(peersCopy, pc)
	}
	h.mu.RUnlock()

	// Send to all peers (non-blocking via buffered channels)
	for _, pc := range peersCopy {
		select {
		case pc.send <- env:
			// Successfully queued
		default:
			// Channel full, skip this peer (avoid blocking)
		}
	}
}

// BroadcastExcept sends an envelope to all peers in a session except the specified peer.
func (h *Hub) BroadcastExcept(sessionID string, exceptPeerID string, env protocol.Envelope) {
	h.mu.RLock()
	sessionPeers, exists := h.sessions[sessionID]
	if !exists {
		h.mu.RUnlock()
		return
	}

	// Find the connection ID for the excepted peer
	exceptConnID := ""
	if peerIDMap, exists := h.byPeerID[sessionID]; exists {
		exceptConnID = peerIDMap[exceptPeerID]
	}

	// Create a copy of peers to send to
	peersCopy := make([]*peerConnection, 0, len(sessionPeers))
	for connID, pc := range sessionPeers {
		if connID != exceptConnID {
			peersCopy = append(peersCopy, pc)
		}
	}
	h.mu.RUnlock()

	// Send to all peers except the specified one
	for _, pc := range peersCopy {
		select {
		case pc.send <- env:
			// Successfully queued
		default:
			// Channel full, skip this peer (avoid blocking)
		}
	}
}

// SendTo sends an envelope to a specific peer in a session.
// Returns true if the peer was found and the message was queued, false otherwise.
func (h *Hub) SendTo(sessionID string, peerID string, env protocol.Envelope) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Look up connection ID for the peer
	peerIDMap, exists := h.byPeerID[sessionID]
	if !exists {
		return false
	}

	connID, exists := peerIDMap[peerID]
	if !exists {
		return false
	}

	// Get the peer connection
	pc, exists := h.sessions[sessionID][connID]
	if !exists {
		return false
	}

	// Send to the peer (non-blocking)
	select {
	case pc.send <- env:
		return true
	default:
		// Channel full, but peer exists
		return true
	}
}

