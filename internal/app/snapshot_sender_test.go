package app

import (
	"context"
	"testing"
	"time"

	"github.com/sheerbytes/sheerbytes/pkg/protocol"
)

func newTestSender(now time.Time, maxReceivers int) *SnapshotSender {
	s := &SnapshotSender{
		maxRecv:     maxReceivers,
		receiverTTL: 10 * time.Minute,
		receivers:   make(map[string]*ReceiverState),
		active:      make(map[string]*transferSlot),
		signalCh:    make(map[string]chan protocol.Envelope),
		now:         func() time.Time { return now },
		exitFn:      func(int) {},
		closeConn:   func() {},
	}
	return s
}

func TestSnapshotCleanupTTL(t *testing.T) {
	now := time.Now()
	s := newTestSender(now, 1)
	s.receivers["old"] = &ReceiverState{PeerID: "old", LastSeen: now.Add(-11 * time.Minute), Status: ReceiverStatusDone}
	s.receivers["active"] = &ReceiverState{PeerID: "active", LastSeen: now.Add(-2 * time.Hour), Status: ReceiverStatusTransferring}
	s.queue = []string{"old", "active"}

	s.cleanup()

	if _, ok := s.receivers["old"]; ok {
		t.Fatalf("expected old receiver to be removed")
	}
	if _, ok := s.receivers["active"]; !ok {
		t.Fatalf("expected active receiver to remain")
	}
	if len(s.queue) != 1 || s.queue[0] != "active" {
		t.Fatalf("expected queue to keep active receiver, got %v", s.queue)
	}
}

func TestSnapshotQueueRespected(t *testing.T) {
	now := time.Now()
	s := newTestSender(now, 1)
	started := make(chan string, 2)
	done := make(chan struct{})
	s.transferFn = func(ctx context.Context, peerID string) error {
		started <- peerID
		<-done
		return nil
	}

	s.handlePeerJoined("a")
	s.handleManifestAccept("a", protocol.ManifestAccept{})
	s.maybeStartTransfers(context.Background())

	select {
	case peer := <-started:
		if peer != "a" {
			t.Fatalf("expected transfer for a, got %s", peer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first transfer")
	}

	s.handlePeerJoined("b")
	s.handleManifestAccept("b", protocol.ManifestAccept{})
	if len(s.queue) != 1 || s.queue[0] != "b" {
		t.Fatalf("expected b queued, got %v", s.queue)
	}

	close(done)
	select {
	case peer := <-started:
		if peer != "b" {
			t.Fatalf("expected transfer for b, got %s", peer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second transfer")
	}
}

func TestSnapshotConcurrencyRespected(t *testing.T) {
	now := time.Now()
	s := newTestSender(now, 2)
	started := make(chan string, 3)
	block := make(chan struct{})
	s.transferFn = func(ctx context.Context, peerID string) error {
		started <- peerID
		<-block
		return nil
	}

	for _, peer := range []string{"a", "b", "c"} {
		s.handlePeerJoined(peer)
		s.handleManifestAccept(peer, protocol.ManifestAccept{})
	}
	s.maybeStartTransfers(context.Background())

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case peer := <-started:
			seen[peer] = true
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for transfers")
		}
	}

	if len(s.active) != 2 {
		t.Fatalf("expected 2 active transfers, got %d", len(s.active))
	}
	if len(s.queue) != 1 {
		t.Fatalf("expected 1 queued transfer, got %d", len(s.queue))
	}
	close(block)
}

func TestSnapshotLateJoin(t *testing.T) {
	now := time.Now()
	s := newTestSender(now, 1)
	started := make(chan string, 1)
	s.transferFn = func(ctx context.Context, peerID string) error {
		started <- peerID
		return nil
	}

	s.handlePeerJoined("late")
	s.handleManifestAccept("late", protocol.ManifestAccept{})
	s.maybeStartTransfers(context.Background())

	select {
	case peer := <-started:
		if peer != "late" {
			t.Fatalf("expected late transfer, got %s", peer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for late transfer")
	}
}

func TestSnapshotHardStopCancelsTransfers(t *testing.T) {
	now := time.Now()
	s := newTestSender(now, 1)
	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(canceled)
	}()

	s.mu.Lock()
	s.active["peer"] = &transferSlot{peerID: "peer", cancel: cancel}
	s.mu.Unlock()

	s.hardStop()

	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected transfer cancel on hard stop")
	}
}

func TestSnapshotPeerLeftMarksFailed(t *testing.T) {
	now := time.Now()
	s := newTestSender(now, 1)
	canceled := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		close(canceled)
	}()

	s.mu.Lock()
	s.receivers["peer"] = &ReceiverState{PeerID: "peer", LastSeen: now, Status: ReceiverStatusTransferring}
	s.active["peer"] = &transferSlot{peerID: "peer", cancel: cancel}
	s.queue = []string{"peer"}
	s.signalCh["peer"] = make(chan protocol.Envelope, 1)
	s.mu.Unlock()

	s.handlePeerLeft("peer")

	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected transfer cancel on peer left")
	}

	s.mu.Lock()
	state := s.receivers["peer"]
	active := s.active["peer"]
	s.mu.Unlock()
	if state == nil || state.Status != ReceiverStatusFailed {
		t.Fatalf("expected failed state, got %+v", state)
	}
	if active != nil {
		t.Fatalf("expected active transfer removed")
	}
}
