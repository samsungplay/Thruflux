package appstate

import (
	"fmt"
	"sync"
	"time"
)

// ReceiverState tracks the state of a receiver peer.
type ReceiverState struct {
	PeerID            string
	AcceptedManifestID string
	Role              string
	LastSeen          time.Time
}

// SenderState manages the state of receivers for a sender.
type SenderState struct {
	mu                sync.RWMutex
	currentManifestID string
	receivers         map[string]*ReceiverState // peer_id -> state
	onOfferSend       func(peerID string) error // callback for sending offers
}

// NewSenderState creates a new sender state tracker.
func NewSenderState(onOfferSend func(peerID string) error) *SenderState {
	return &SenderState{
		receivers:   make(map[string]*ReceiverState),
		onOfferSend: onOfferSend,
	}
}

// SetCurrentManifest sets the current manifest ID.
func (s *SenderState) SetCurrentManifest(manifestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentManifestID = manifestID
}

// CurrentManifest returns the current manifest ID.
func (s *SenderState) CurrentManifest() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentManifestID
}

// UpdatePeerList updates receiver states from a peer list.
// roleMap maps peer_id -> role.
func (s *SenderState) UpdatePeerList(roleMap map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	// Update existing receivers and add new ones
	for peerID, role := range roleMap {
		if state, exists := s.receivers[peerID]; exists {
			state.Role = role
			state.LastSeen = now
		} else {
			s.receivers[peerID] = &ReceiverState{
				PeerID:            peerID,
				AcceptedManifestID: "",
				Role:              role,
				LastSeen:          now,
			}
		}
	}
}

// HandlePeerJoined handles a peer joined event.
// If a manifest exists, sends offer directly to the new peer.
func (s *SenderState) HandlePeerJoined(peerID, role string) error {
	s.mu.Lock()
	now := time.Now()

	// Add or update receiver state
	if state, exists := s.receivers[peerID]; exists {
		state.Role = role
		state.LastSeen = now
	} else {
		s.receivers[peerID] = &ReceiverState{
			PeerID:            peerID,
			AcceptedManifestID: "",
			Role:              role,
			LastSeen:          now,
		}
	}

	// If manifest exists and peer is a receiver, send direct offer
	manifestID := s.currentManifestID
	onOfferSend := s.onOfferSend
	s.mu.Unlock()

	if manifestID != "" && role == "receiver" && onOfferSend != nil {
		return onOfferSend(peerID)
	}

	return nil
}

// HandlePeerLeft handles a peer left event by removing the receiver state.
func (s *SenderState) HandlePeerLeft(peerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.receivers, peerID)
}

// HandleAccept records a manifest acceptance from a receiver.
func (s *SenderState) HandleAccept(peerID, manifestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if state, exists := s.receivers[peerID]; exists {
		state.AcceptedManifestID = manifestID
		state.LastSeen = time.Now()
	}
}

// ReadyReceivers returns peer IDs of receivers that have accepted the current manifest.
func (s *SenderState) ReadyReceivers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ready := make([]string, 0)
	for peerID, state := range s.receivers {
		if state.Role == "receiver" && state.AcceptedManifestID == s.currentManifestID {
			ready = append(ready, peerID)
		}
	}

	// Sort for deterministic output
	for i := 0; i < len(ready)-1; i++ {
		for j := i + 1; j < len(ready); j++ {
			if ready[i] > ready[j] {
				ready[i], ready[j] = ready[j], ready[i]
			}
		}
	}

	return ready
}

// TotalReceivers returns the total number of receivers.
func (s *SenderState) TotalReceivers() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, state := range s.receivers {
		if state.Role == "receiver" {
			count++
		}
	}
	return count
}

// ReadinessInfo returns a formatted readiness string.
func (s *SenderState) ReadinessInfo() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ready := s.readyReceiversLocked()
	total := 0
	for _, state := range s.receivers {
		if state.Role == "receiver" {
			total++
		}
	}

	manifestID := s.currentManifestID
	if manifestID == "" {
		manifestID = "none"
	}

	if len(ready) <= 10 {
		return fmt.Sprintf("READY receivers: %d/%d (manifest %s) %v", len(ready), total, manifestID, ready)
	}
	return fmt.Sprintf("READY receivers: %d/%d (manifest %s)", len(ready), total, manifestID)
}

// readyReceiversLocked returns ready receiver IDs (must be called with lock held).
func (s *SenderState) readyReceiversLocked() []string {
	ready := make([]string, 0)
	for peerID, state := range s.receivers {
		if state.Role == "receiver" && state.AcceptedManifestID == s.currentManifestID {
			ready = append(ready, peerID)
		}
	}

	// Sort for deterministic output
	for i := 0; i < len(ready)-1; i++ {
		for j := i + 1; j < len(ready); j++ {
			if ready[i] > ready[j] {
				ready[i], ready[j] = ready[j], ready[i]
			}
		}
	}

	return ready
}

