package appstate

import (
	"errors"
	"testing"
)

func TestNewSenderState(t *testing.T) {
	s := NewSenderState(nil)
	if s == nil {
		t.Fatal("NewSenderState returned nil")
	}
	if s.CurrentManifest() != "" {
		t.Errorf("CurrentManifest = %s, want empty string", s.CurrentManifest())
	}
	if s.TotalReceivers() != 0 {
		t.Errorf("TotalReceivers = %d, want 0", s.TotalReceivers())
	}
}

func TestSetCurrentManifest(t *testing.T) {
	s := NewSenderState(nil)
	manifestID := "test-manifest-123"
	s.SetCurrentManifest(manifestID)

	if s.CurrentManifest() != manifestID {
		t.Errorf("CurrentManifest = %s, want %s", s.CurrentManifest(), manifestID)
	}
}

func TestUpdatePeerList(t *testing.T) {
	s := NewSenderState(nil)
	roleMap := map[string]string{
		"receiver1": "receiver",
		"receiver2": "receiver",
		"sender":    "sender",
	}

	s.UpdatePeerList(roleMap)

	if s.TotalReceivers() != 2 {
		t.Errorf("TotalReceivers = %d, want 2", s.TotalReceivers())
	}
}

func TestHandlePeerJoined_WithManifest(t *testing.T) {
	offerSent := false
	offerPeerID := ""

	onOfferSend := func(peerID string) error {
		offerSent = true
		offerPeerID = peerID
		return nil
	}

	s := NewSenderState(onOfferSend)
	s.SetCurrentManifest("manifest-123")

	err := s.HandlePeerJoined("receiver1", "receiver")
	if err != nil {
		t.Fatalf("HandlePeerJoined error = %v", err)
	}

	if !offerSent {
		t.Error("Expected offer to be sent")
	}
	if offerPeerID != "receiver1" {
		t.Errorf("Offer sent to peer_id = %s, want receiver1", offerPeerID)
	}
}

func TestHandlePeerJoined_WithoutManifest(t *testing.T) {
	offerSent := false

	onOfferSend := func(peerID string) error {
		offerSent = true
		return nil
	}

	s := NewSenderState(onOfferSend)
	// No manifest set

	err := s.HandlePeerJoined("receiver1", "receiver")
	if err != nil {
		t.Fatalf("HandlePeerJoined error = %v", err)
	}

	if offerSent {
		t.Error("Expected offer NOT to be sent when no manifest exists")
	}
}

func TestHandlePeerJoined_SenderRole(t *testing.T) {
	offerSent := false

	onOfferSend := func(peerID string) error {
		offerSent = true
		return nil
	}

	s := NewSenderState(onOfferSend)
	s.SetCurrentManifest("manifest-123")

	err := s.HandlePeerJoined("sender1", "sender")
	if err != nil {
		t.Fatalf("HandlePeerJoined error = %v", err)
	}

	if offerSent {
		t.Error("Expected offer NOT to be sent for sender role")
	}
}

func TestHandleAccept(t *testing.T) {
	s := NewSenderState(nil)
	s.SetCurrentManifest("manifest-123")

	roleMap := map[string]string{
		"receiver1": "receiver",
		"receiver2": "receiver",
	}
	s.UpdatePeerList(roleMap)

	// Accept from receiver1
	s.HandleAccept("receiver1", "manifest-123")

	ready := s.ReadyReceivers()
	if len(ready) != 1 {
		t.Errorf("ReadyReceivers count = %d, want 1", len(ready))
	}
	if ready[0] != "receiver1" {
		t.Errorf("ReadyReceivers[0] = %s, want receiver1", ready[0])
	}

	// Accept from receiver2
	s.HandleAccept("receiver2", "manifest-123")

	ready = s.ReadyReceivers()
	if len(ready) != 2 {
		t.Errorf("ReadyReceivers count = %d, want 2", len(ready))
	}
}

func TestHandleAccept_WrongManifest(t *testing.T) {
	s := NewSenderState(nil)
	s.SetCurrentManifest("manifest-123")

	roleMap := map[string]string{
		"receiver1": "receiver",
	}
	s.UpdatePeerList(roleMap)

	// Accept wrong manifest
	s.HandleAccept("receiver1", "manifest-456")

	ready := s.ReadyReceivers()
	if len(ready) != 0 {
		t.Errorf("ReadyReceivers count = %d, want 0 (wrong manifest)", len(ready))
	}
}

func TestReadyReceivers(t *testing.T) {
	s := NewSenderState(nil)
	s.SetCurrentManifest("manifest-123")

	roleMap := map[string]string{
		"receiver1": "receiver",
		"receiver2": "receiver",
		"receiver3": "receiver",
		"sender":    "sender",
	}
	s.UpdatePeerList(roleMap)

	// Only receiver1 and receiver2 accept
	s.HandleAccept("receiver1", "manifest-123")
	s.HandleAccept("receiver2", "manifest-123")
	s.HandleAccept("receiver3", "manifest-456") // Wrong manifest

	ready := s.ReadyReceivers()
	if len(ready) != 2 {
		t.Errorf("ReadyReceivers count = %d, want 2", len(ready))
	}

	// Should be sorted
	if ready[0] != "receiver1" || ready[1] != "receiver2" {
		t.Errorf("ReadyReceivers = %v, want [receiver1 receiver2]", ready)
	}
}

func TestHandlePeerLeft(t *testing.T) {
	s := NewSenderState(nil)
	roleMap := map[string]string{
		"receiver1": "receiver",
	}
	s.UpdatePeerList(roleMap)

	if s.TotalReceivers() != 1 {
		t.Errorf("TotalReceivers = %d, want 1", s.TotalReceivers())
	}

	s.HandlePeerLeft("receiver1")

	if s.TotalReceivers() != 0 {
		t.Errorf("TotalReceivers = %d, want 0 after peer left", s.TotalReceivers())
	}
}

func TestReadinessInfo(t *testing.T) {
	s := NewSenderState(nil)
	s.SetCurrentManifest("manifest-123")

	roleMap := map[string]string{
		"receiver1": "receiver",
		"receiver2": "receiver",
	}
	s.UpdatePeerList(roleMap)

	// No accepts yet
	info := s.ReadinessInfo()
	expected := "READY receivers: 0/2 (manifest manifest-123) []"
	if info != expected {
		t.Errorf("ReadinessInfo = %s, want %s", info, expected)
	}

	// One accepts
	s.HandleAccept("receiver1", "manifest-123")
	info = s.ReadinessInfo()
	// Should contain receiver1 in the list
	if len(s.ReadyReceivers()) != 1 {
		t.Errorf("Expected 1 ready receiver")
	}
}

func TestReadinessInfo_NoManifest(t *testing.T) {
	s := NewSenderState(nil)
	// No manifest set

	info := s.ReadinessInfo()
	expected := "READY receivers: 0/0 (manifest none) []"
	if info != expected {
		t.Errorf("ReadinessInfo = %s, want %s", info, expected)
	}
}

func TestHandlePeerJoined_OfferSendError(t *testing.T) {
	onOfferSend := func(peerID string) error {
		return errors.New("send failed")
	}

	s := NewSenderState(onOfferSend)
	s.SetCurrentManifest("manifest-123")

	err := s.HandlePeerJoined("receiver1", "receiver")
	if err == nil {
		t.Error("Expected error when offer send fails")
	}
}

func TestLateJoin_TriggersDirectOffer(t *testing.T) {
	offersSent := make([]string, 0)

	onOfferSend := func(peerID string) error {
		offersSent = append(offersSent, peerID)
		return nil
	}

	s := NewSenderState(onOfferSend)
	s.SetCurrentManifest("manifest-123")

	// Initial peer list with one receiver
	roleMap1 := map[string]string{
		"receiver1": "receiver",
	}
	s.UpdatePeerList(roleMap1)

	// Later, receiver2 joins
	err := s.HandlePeerJoined("receiver2", "receiver")
	if err != nil {
		t.Fatalf("HandlePeerJoined error = %v", err)
	}

	// Should have sent offer to receiver2
	if len(offersSent) != 1 {
		t.Fatalf("Expected 1 offer sent, got %d", len(offersSent))
	}
	if offersSent[0] != "receiver2" {
		t.Errorf("Offer sent to %s, want receiver2", offersSent[0])
	}
}

func TestAccept_IncrementsReadiness(t *testing.T) {
	s := NewSenderState(nil)
	s.SetCurrentManifest("manifest-123")

	roleMap := map[string]string{
		"receiver1": "receiver",
		"receiver2": "receiver",
		"receiver3": "receiver",
	}
	s.UpdatePeerList(roleMap)

	// Initially no ready receivers
	ready := s.ReadyReceivers()
	if len(ready) != 0 {
		t.Errorf("Initial ReadyReceivers count = %d, want 0", len(ready))
	}

	// Accept from receiver1
	s.HandleAccept("receiver1", "manifest-123")
	ready = s.ReadyReceivers()
	if len(ready) != 1 {
		t.Errorf("After first accept, ReadyReceivers count = %d, want 1", len(ready))
	}

	// Accept from receiver2
	s.HandleAccept("receiver2", "manifest-123")
	ready = s.ReadyReceivers()
	if len(ready) != 2 {
		t.Errorf("After second accept, ReadyReceivers count = %d, want 2", len(ready))
	}

	// Accept from receiver3
	s.HandleAccept("receiver3", "manifest-123")
	ready = s.ReadyReceivers()
	if len(ready) != 3 {
		t.Errorf("After third accept, ReadyReceivers count = %d, want 3", len(ready))
	}
}

