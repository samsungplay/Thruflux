package peers

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sheerbytes/sheerbytes/pkg/protocol"
)

func TestHub_Add(t *testing.T) {
	hub := NewHub()

	var sentEnvelopes []protocol.Envelope
	var mu sync.Mutex

	sendFunc := func(env protocol.Envelope) error {
		mu.Lock()
		defer mu.Unlock()
		sentEnvelopes = append(sentEnvelopes, env)
		return nil
	}

	peer := Peer{
		PeerID: "peer1",
		Role:   "sender",
		ConnID: "conn1",
	}

	remove := hub.Add("session1", peer, sendFunc)

	// Verify peer is in the list
	peers := hub.List("session1")
	if len(peers) != 1 {
		t.Fatalf("Expected 1 peer, got %d", len(peers))
	}
	if peers[0].PeerID != "peer1" {
		t.Errorf("PeerID = %s, want peer1", peers[0].PeerID)
	}
	if peers[0].Role != "sender" {
		t.Errorf("Role = %s, want sender", peers[0].Role)
	}

	// Remove peer
	remove()

	// Verify peer is removed
	peers = hub.List("session1")
	if len(peers) != 0 {
		t.Errorf("Expected 0 peers after remove, got %d", len(peers))
	}
}

func TestHub_List(t *testing.T) {
	hub := NewHub()

	sendFunc := func(env protocol.Envelope) error { return nil }

	// Add multiple peers to same session
	hub.Add("session1", Peer{PeerID: "peer1", Role: "sender", ConnID: "conn1"}, sendFunc)
	hub.Add("session1", Peer{PeerID: "peer2", Role: "receiver", ConnID: "conn2"}, sendFunc)
	hub.Add("session1", Peer{PeerID: "peer3", Role: "sender", ConnID: "conn3"}, sendFunc)

	// Add peer to different session
	hub.Add("session2", Peer{PeerID: "peer4", Role: "sender", ConnID: "conn4"}, sendFunc)

	peers := hub.List("session1")
	if len(peers) != 3 {
		t.Fatalf("Expected 3 peers in session1, got %d", len(peers))
	}

	// Verify all peers are present
	peerMap := make(map[string]string)
	for _, p := range peers {
		peerMap[p.PeerID] = p.Role
	}

	if peerMap["peer1"] != "sender" {
		t.Error("peer1 not found or wrong role")
	}
	if peerMap["peer2"] != "receiver" {
		t.Error("peer2 not found or wrong role")
	}
	if peerMap["peer3"] != "sender" {
		t.Error("peer3 not found or wrong role")
	}

	// Verify session2 is separate
	peers2 := hub.List("session2")
	if len(peers2) != 1 {
		t.Fatalf("Expected 1 peer in session2, got %d", len(peers2))
	}
	if peers2[0].PeerID != "peer4" {
		t.Errorf("PeerID = %s, want peer4", peers2[0].PeerID)
	}

	// Verify empty session returns empty list
	peers3 := hub.List("nonexistent")
	if len(peers3) != 0 {
		t.Errorf("Expected 0 peers for nonexistent session, got %d", len(peers3))
	}
}

func TestHub_Broadcast(t *testing.T) {
	hub := NewHub()

	var sent1, sent2, sent3 []protocol.Envelope
	var mu1, mu2, mu3 sync.Mutex

	sendFunc1 := func(env protocol.Envelope) error {
		mu1.Lock()
		defer mu1.Unlock()
		sent1 = append(sent1, env)
		return nil
	}
	sendFunc2 := func(env protocol.Envelope) error {
		mu2.Lock()
		defer mu2.Unlock()
		sent2 = append(sent2, env)
		return nil
	}
	sendFunc3 := func(env protocol.Envelope) error {
		mu3.Lock()
		defer mu3.Unlock()
		sent3 = append(sent3, env)
		return nil
	}

	// Add peers to session
	hub.Add("session1", Peer{PeerID: "peer1", Role: "sender", ConnID: "conn1"}, sendFunc1)
	hub.Add("session1", Peer{PeerID: "peer2", Role: "receiver", ConnID: "conn2"}, sendFunc2)
	hub.Add("session2", Peer{PeerID: "peer3", Role: "sender", ConnID: "conn3"}, sendFunc3)

	// Create test envelope
	env, err := protocol.NewEnvelope(protocol.TypePeerJoined, protocol.NewMsgID(), protocol.PeerJoined{
		Peer: protocol.PeerInfo{PeerID: "peer4", Role: "sender"},
	})
	if err != nil {
		t.Fatalf("NewEnvelope error = %v", err)
	}

	// Broadcast to session1
	hub.Broadcast("session1", env)

	// Wait a bit for goroutines to process messages
	time.Sleep(100 * time.Millisecond)

	mu1.Lock()
	mu2.Lock()
	count1 := len(sent1)
	count2 := len(sent2)
	mu1.Unlock()
	mu2.Unlock()

	if count1 != 1 {
		t.Errorf("peer1 received %d messages, want 1", count1)
	}
	if count2 != 1 {
		t.Errorf("peer2 received %d messages, want 1", count2)
	}

	// Verify session2 didn't receive the message
	mu3.Lock()
	count3 := len(sent3)
	mu3.Unlock()
	if count3 != 0 {
		t.Errorf("peer3 received %d messages, want 0 (different session)", count3)
	}

	// Verify message content
	mu1.Lock()
	if len(sent1) > 0 {
		if sent1[0].Type != protocol.TypePeerJoined {
			t.Errorf("Message type = %s, want %s", sent1[0].Type, protocol.TypePeerJoined)
		}
	}
	mu1.Unlock()
}

func TestHub_Broadcast_NonBlocking(t *testing.T) {
	hub := NewHub()

	// Use a send function that blocks forever to test non-blocking behavior
	blockingSend := func(env protocol.Envelope) error {
		<-make(chan struct{}) // Block forever
		return nil
	}

	// Add peer with blocking send
	hub.Add("session1", Peer{PeerID: "peer1", Role: "sender", ConnID: "conn1"}, blockingSend)

	// Create many envelopes
	for i := 0; i < 300; i++ { // More than buffer size (256)
		env, err := protocol.NewEnvelope(protocol.TypePeerList, protocol.NewMsgID(), protocol.PeerList{})
		if err != nil {
			t.Fatalf("NewEnvelope error = %v", err)
		}
		// This should not block even though send is blocked
		hub.Broadcast("session1", env)
	}

	// If we get here, the non-blocking behavior works
}

func TestHub_ConcurrentAccess(t *testing.T) {
	hub := NewHub()
	done := make(chan bool)

	sendFunc := func(env protocol.Envelope) error { return nil }

	// Concurrent adds
	go func() {
		for i := 0; i < 50; i++ {
			peer := Peer{PeerID: "peer1", Role: "sender", ConnID: "conn1"}
			hub.Add("session1", peer, sendFunc)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 50; i++ {
			hub.List("session1")
		}
		done <- true
	}()

	go func() {
		env, _ := protocol.NewEnvelope(protocol.TypePeerList, protocol.NewMsgID(), protocol.PeerList{})
		for i := 0; i < 50; i++ {
			hub.Broadcast("session1", env)
		}
		done <- true
	}()

	// Wait for all goroutines
	for i := 0; i < 3; i++ {
		<-done
	}

	// Should not have panicked or deadlocked
}

func TestHub_SendTo(t *testing.T) {
	hub := NewHub()

	var sent1, sent2, sent3 []protocol.Envelope
	var mu1, mu2, mu3 sync.Mutex

	sendFunc1 := func(env protocol.Envelope) error {
		mu1.Lock()
		defer mu1.Unlock()
		sent1 = append(sent1, env)
		return nil
	}
	sendFunc2 := func(env protocol.Envelope) error {
		mu2.Lock()
		defer mu2.Unlock()
		sent2 = append(sent2, env)
		return nil
	}
	sendFunc3 := func(env protocol.Envelope) error {
		mu3.Lock()
		defer mu3.Unlock()
		sent3 = append(sent3, env)
		return nil
	}

	// Add peers to session
	hub.Add("session1", Peer{PeerID: "peer1", Role: "sender", ConnID: "conn1"}, sendFunc1)
	hub.Add("session1", Peer{PeerID: "peer2", Role: "receiver", ConnID: "conn2"}, sendFunc2)
	hub.Add("session2", Peer{PeerID: "peer3", Role: "sender", ConnID: "conn3"}, sendFunc3)

	// Create test envelope
	env, err := protocol.NewEnvelope(protocol.TypeOffer, protocol.NewMsgID(), protocol.Offer{SDP: "test-sdp"})
	if err != nil {
		t.Fatalf("NewEnvelope error = %v", err)
	}

	// Send to peer1
	sent := hub.SendTo("session1", "peer1", env)
	if !sent {
		t.Error("SendTo should return true when peer exists")
	}

	time.Sleep(50 * time.Millisecond)

	// Verify only peer1 received the message
	mu1.Lock()
	mu2.Lock()
	mu3.Lock()
	count1 := len(sent1)
	count2 := len(sent2)
	count3 := len(sent3)
	mu1.Unlock()
	mu2.Unlock()
	mu3.Unlock()

	if count1 != 1 {
		t.Errorf("peer1 received %d messages, want 1", count1)
	}
	if count2 != 0 {
		t.Errorf("peer2 received %d messages, want 0", count2)
	}
	if count3 != 0 {
		t.Errorf("peer3 received %d messages, want 0 (different session)", count3)
	}

	// Try to send to non-existent peer
	sent = hub.SendTo("session1", "nonexistent", env)
	if sent {
		t.Error("SendTo should return false when peer does not exist")
	}

	// Try to send to peer in different session
	sent = hub.SendTo("session1", "peer3", env)
	if sent {
		t.Error("SendTo should return false when peer is in different session")
	}
}

func TestHub_BroadcastExcept(t *testing.T) {
	hub := NewHub()

	var sent1, sent2, sent3 []protocol.Envelope
	var mu1, mu2, mu3 sync.Mutex

	sendFunc1 := func(env protocol.Envelope) error {
		mu1.Lock()
		defer mu1.Unlock()
		sent1 = append(sent1, env)
		return nil
	}
	sendFunc2 := func(env protocol.Envelope) error {
		mu2.Lock()
		defer mu2.Unlock()
		sent2 = append(sent2, env)
		return nil
	}
	sendFunc3 := func(env protocol.Envelope) error {
		mu3.Lock()
		defer mu3.Unlock()
		sent3 = append(sent3, env)
		return nil
	}

	// Add peers to session
	hub.Add("session1", Peer{PeerID: "peer1", Role: "sender", ConnID: "conn1"}, sendFunc1)
	hub.Add("session1", Peer{PeerID: "peer2", Role: "receiver", ConnID: "conn2"}, sendFunc2)
	hub.Add("session1", Peer{PeerID: "peer3", Role: "sender", ConnID: "conn3"}, sendFunc3)

	// Create test envelope
	env, err := protocol.NewEnvelope(protocol.TypeAnswer, protocol.NewMsgID(), protocol.Answer{SDP: "test-answer"})
	if err != nil {
		t.Fatalf("NewEnvelope error = %v", err)
	}

	// Broadcast except peer2
	hub.BroadcastExcept("session1", "peer2", env)

	time.Sleep(50 * time.Millisecond)

	// Verify peer1 and peer3 received, but peer2 did not
	mu1.Lock()
	mu2.Lock()
	mu3.Lock()
	count1 := len(sent1)
	count2 := len(sent2)
	count3 := len(sent3)
	mu1.Unlock()
	mu2.Unlock()
	mu3.Unlock()

	if count1 != 1 {
		t.Errorf("peer1 received %d messages, want 1", count1)
	}
	if count2 != 0 {
		t.Errorf("peer2 received %d messages, want 0 (excluded)", count2)
	}
	if count3 != 1 {
		t.Errorf("peer3 received %d messages, want 1", count3)
	}
}

func TestHub_DuplicatePeerID_LastWriteWins(t *testing.T) {
	hub := NewHub()

	var sent1, sent2 []protocol.Envelope
	var mu1, mu2 sync.Mutex

	sendFunc1 := func(env protocol.Envelope) error {
		mu1.Lock()
		defer mu1.Unlock()
		sent1 = append(sent1, env)
		return nil
	}
	sendFunc2 := func(env protocol.Envelope) error {
		mu2.Lock()
		defer mu2.Unlock()
		sent2 = append(sent2, env)
		return nil
	}

	// Add peer with same peer_id but different conn_id (first connection)
	remove1 := hub.Add("session1", Peer{PeerID: "peer1", Role: "sender", ConnID: "conn1"}, sendFunc1)

	// Add peer with same peer_id but different conn_id (second connection - should replace)
	remove2 := hub.Add("session1", Peer{PeerID: "peer1", Role: "sender", ConnID: "conn2"}, sendFunc2)

	// Verify only one peer in the list
	peers := hub.List("session1")
	if len(peers) != 1 {
		t.Errorf("Expected 1 peer in list, got %d", len(peers))
	}

	// Send a message to peer1 - should go to the new connection (conn2)
	env, err := protocol.NewEnvelope(protocol.TypeOffer, protocol.NewMsgID(), protocol.Offer{SDP: "test"})
	if err != nil {
		t.Fatalf("NewEnvelope error = %v", err)
	}

	hub.SendTo("session1", "peer1", env)

	time.Sleep(50 * time.Millisecond)

	// Verify message went to conn2 (sendFunc2), not conn1 (sendFunc1)
	mu1.Lock()
	mu2.Lock()
	count1 := len(sent1)
	count2 := len(sent2)
	mu1.Unlock()
	mu2.Unlock()

	if count1 != 0 {
		t.Errorf("Old connection received %d messages, want 0", count1)
	}
	if count2 != 1 {
		t.Errorf("New connection received %d messages, want 1", count2)
	}

	// Clean up
	remove1()
	remove2()
}

func TestHub_RemoveOnError(t *testing.T) {
	hub := NewHub()

	var sendCount int
	var mu sync.Mutex

	sendFunc := func(env protocol.Envelope) error {
		mu.Lock()
		defer mu.Unlock()
		sendCount++
		if sendCount == 1 {
			return errors.New("connection closed")
		}
		return nil
	}

	peer := Peer{PeerID: "peer1", Role: "sender", ConnID: "conn1"}
	remove := hub.Add("session1", peer, sendFunc)

	// Send a message that will cause error
	env, err := protocol.NewEnvelope(protocol.TypePeerList, protocol.NewMsgID(), protocol.PeerList{})
	if err != nil {
		t.Fatalf("NewEnvelope error = %v", err)
	}

	hub.Broadcast("session1", env)

	// Remove peer
	remove()

	// Verify peer is removed
	peers := hub.List("session1")
	if len(peers) != 0 {
		t.Errorf("Expected 0 peers after remove, got %d", len(peers))
	}
}

