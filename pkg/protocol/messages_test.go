package protocol

import (
	"encoding/json"
	"testing"
)

func TestMessageTypes_JSONRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		msgType  string
		payload  any
		decodeTo func() any // factory function to create empty instance
		verify   func(t *testing.T, decoded, original any)
	}{
		// Peer events
		{
			name:    "PeerJoined",
			msgType: TypePeerJoined,
			payload: PeerJoined{
				Peer: PeerInfo{
					PeerID: "peer1",
					Role:   "sender",
				},
			},
			decodeTo: func() any { return &PeerJoined{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*PeerJoined)
				o := original.(PeerJoined)
				if d.Peer.PeerID != o.Peer.PeerID {
					t.Errorf("Peer.PeerID = %s, want %s", d.Peer.PeerID, o.Peer.PeerID)
				}
				if d.Peer.Role != o.Peer.Role {
					t.Errorf("Peer.Role = %s, want %s", d.Peer.Role, o.Peer.Role)
				}
			},
		},
		{
			name:    "PeerLeft",
			msgType: TypePeerLeft,
			payload: PeerLeft{
				PeerID: "peer1",
			},
			decodeTo: func() any { return &PeerLeft{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*PeerLeft)
				o := original.(PeerLeft)
				if d.PeerID != o.PeerID {
					t.Errorf("PeerID = %s, want %s", d.PeerID, o.PeerID)
				}
			},
		},
		// Session/join
		{
			name:    "CreateSessionRequest",
			msgType: TypeCreateSessionRequest,
			payload: CreateSessionRequest{
				Role: "sender",
			},
			decodeTo: func() any { return &CreateSessionRequest{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*CreateSessionRequest)
				o := original.(CreateSessionRequest)
				if d.Role != o.Role {
					t.Errorf("Role = %s, want %s", d.Role, o.Role)
				}
			},
		},
		{
			name:     "CreateSessionRequest_empty",
			msgType:  TypeCreateSessionRequest,
			payload:  CreateSessionRequest{},
			decodeTo: func() any { return &CreateSessionRequest{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*CreateSessionRequest)
				o := original.(CreateSessionRequest)
				if d.Role != o.Role {
					t.Errorf("Role = %s, want %s", d.Role, o.Role)
				}
			},
		},
		{
			name:    "CreateSessionResponse",
			msgType: TypeCreateSessionResponse,
			payload: CreateSessionResponse{
				SessionID: "session123",
				JoinCode:  "ABC123",
			},
			decodeTo: func() any { return &CreateSessionResponse{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*CreateSessionResponse)
				o := original.(CreateSessionResponse)
				if d.SessionID != o.SessionID {
					t.Errorf("SessionID = %s, want %s", d.SessionID, o.SessionID)
				}
				if d.JoinCode != o.JoinCode {
					t.Errorf("JoinCode = %s, want %s", d.JoinCode, o.JoinCode)
				}
			},
		},
		{
			name:    "JoinSession",
			msgType: TypeJoinSession,
			payload: JoinSession{
				JoinCode: "ABC123",
				PeerID:   "peer1",
				Role:     "receiver",
			},
			decodeTo: func() any { return &JoinSession{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*JoinSession)
				o := original.(JoinSession)
				if d.JoinCode != o.JoinCode {
					t.Errorf("JoinCode = %s, want %s", d.JoinCode, o.JoinCode)
				}
				if d.PeerID != o.PeerID {
					t.Errorf("PeerID = %s, want %s", d.PeerID, o.PeerID)
				}
				if d.Role != o.Role {
					t.Errorf("Role = %s, want %s", d.Role, o.Role)
				}
			},
		},
		// Signaling
		{
			name:    "Offer",
			msgType: TypeOffer,
			payload: Offer{
				SDP: "v=0\r\no=- 123456789 123456789 IN IP4 127.0.0.1\r\n...",
			},
			decodeTo: func() any { return &Offer{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*Offer)
				o := original.(Offer)
				if d.SDP != o.SDP {
					t.Errorf("SDP = %s, want %s", d.SDP, o.SDP)
				}
			},
		},
		{
			name:    "Answer",
			msgType: TypeAnswer,
			payload: Answer{
				SDP: "v=0\r\no=- 987654321 987654321 IN IP4 127.0.0.1\r\n...",
			},
			decodeTo: func() any { return &Answer{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*Answer)
				o := original.(Answer)
				if d.SDP != o.SDP {
					t.Errorf("SDP = %s, want %s", d.SDP, o.SDP)
				}
			},
		},
		{
			name:    "IceCandidate",
			msgType: TypeIceCandidate,
			payload: IceCandidate{
				Candidate: "candidate:1 1 UDP 2130706431 192.168.1.100 54321 typ host",
			},
			decodeTo: func() any { return &IceCandidate{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*IceCandidate)
				o := original.(IceCandidate)
				if d.Candidate != o.Candidate {
					t.Errorf("Candidate = %s, want %s", d.Candidate, o.Candidate)
				}
			},
		},
		{
			name:    "TurnCredentials",
			msgType: TypeTurnCredentials,
			payload: TurnCredentials{
				Servers:   []string{"turn://user:pass@turn.example.com:3478"},
				ExpiresAt: "2030-01-01T00:00:00Z",
			},
			decodeTo: func() any { return &TurnCredentials{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*TurnCredentials)
				o := original.(TurnCredentials)
				if len(d.Servers) != len(o.Servers) {
					t.Fatalf("Servers len = %d, want %d", len(d.Servers), len(o.Servers))
				}
				if len(d.Servers) > 0 && d.Servers[0] != o.Servers[0] {
					t.Errorf("Servers[0] = %s, want %s", d.Servers[0], o.Servers[0])
				}
				if d.ExpiresAt != o.ExpiresAt {
					t.Errorf("ExpiresAt = %s, want %s", d.ExpiresAt, o.ExpiresAt)
				}
			},
		},
		// Manifest
		{
			name:    "ManifestOffer",
			msgType: TypeManifestOffer,
			payload: ManifestOffer{
				Summary: ManifestSummary{
					ManifestID:  "manifest123",
					TotalBytes:  1024 * 1024 * 100,
					FileCount:   42,
					FolderCount: 5,
					RootName:    "my-files",
				},
			},
			decodeTo: func() any { return &ManifestOffer{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*ManifestOffer)
				o := original.(ManifestOffer)
				if d.Summary.ManifestID != o.Summary.ManifestID {
					t.Errorf("Summary.ManifestID = %s, want %s", d.Summary.ManifestID, o.Summary.ManifestID)
				}
				if d.Summary.TotalBytes != o.Summary.TotalBytes {
					t.Errorf("Summary.TotalBytes = %d, want %d", d.Summary.TotalBytes, o.Summary.TotalBytes)
				}
				if d.Summary.FileCount != o.Summary.FileCount {
					t.Errorf("Summary.FileCount = %d, want %d", d.Summary.FileCount, o.Summary.FileCount)
				}
				if d.Summary.FolderCount != o.Summary.FolderCount {
					t.Errorf("Summary.FolderCount = %d, want %d", d.Summary.FolderCount, o.Summary.FolderCount)
				}
				if d.Summary.RootName != o.Summary.RootName {
					t.Errorf("Summary.RootName = %s, want %s", d.Summary.RootName, o.Summary.RootName)
				}
			},
		},
		{
			name:    "ManifestAccept_all",
			msgType: TypeManifestAccept,
			payload: ManifestAccept{
				ManifestID: "manifest123",
				Mode:       "all",
			},
			decodeTo: func() any { return &ManifestAccept{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*ManifestAccept)
				o := original.(ManifestAccept)
				if d.ManifestID != o.ManifestID {
					t.Errorf("ManifestID = %s, want %s", d.ManifestID, o.ManifestID)
				}
				if d.Mode != o.Mode {
					t.Errorf("Mode = %s, want %s", d.Mode, o.Mode)
				}
				if d.SelectedPaths != nil && len(d.SelectedPaths) != 0 {
					t.Errorf("SelectedPaths should be empty, got %v", d.SelectedPaths)
				}
			},
		},
		{
			name:    "ManifestAccept_with_paths",
			msgType: TypeManifestAccept,
			payload: ManifestAccept{
				ManifestID:          "manifest123",
				Mode:                "selective",
				SelectedPaths:       []string{"/path/to/file1.txt", "/path/to/file2.txt"},
				ParallelConnections: 2,
				ParallelStreams:     4,
			},
			decodeTo: func() any { return &ManifestAccept{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*ManifestAccept)
				o := original.(ManifestAccept)
				if d.ManifestID != o.ManifestID {
					t.Errorf("ManifestID = %s, want %s", d.ManifestID, o.ManifestID)
				}
				if d.Mode != o.Mode {
					t.Errorf("Mode = %s, want %s", d.Mode, o.Mode)
				}
				if len(d.SelectedPaths) != len(o.SelectedPaths) {
					t.Fatalf("SelectedPaths length = %d, want %d", len(d.SelectedPaths), len(o.SelectedPaths))
				}
				for i, path := range d.SelectedPaths {
					if path != o.SelectedPaths[i] {
						t.Errorf("SelectedPaths[%d] = %s, want %s", i, path, o.SelectedPaths[i])
					}
				}
				if d.ParallelConnections != o.ParallelConnections {
					t.Errorf("ParallelConnections = %d, want %d", d.ParallelConnections, o.ParallelConnections)
				}
				if d.ParallelStreams != o.ParallelStreams {
					t.Errorf("ParallelStreams = %d, want %d", d.ParallelStreams, o.ParallelStreams)
				}
			},
		},
		{
			name:    "TransferStart",
			msgType: TypeTransferStart,
			payload: TransferStart{
				ManifestID:          "manifest123",
				SenderPeerID:        "sender1",
				ReceiverPeerID:      "receiver1",
				TransferID:          "transfer-abc",
				ParallelConnections: 3,
			},
			decodeTo: func() any { return &TransferStart{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*TransferStart)
				o := original.(TransferStart)
				if d.ManifestID != o.ManifestID {
					t.Errorf("ManifestID = %s, want %s", d.ManifestID, o.ManifestID)
				}
				if d.SenderPeerID != o.SenderPeerID {
					t.Errorf("SenderPeerID = %s, want %s", d.SenderPeerID, o.SenderPeerID)
				}
				if d.ReceiverPeerID != o.ReceiverPeerID {
					t.Errorf("ReceiverPeerID = %s, want %s", d.ReceiverPeerID, o.ReceiverPeerID)
				}
				if d.TransferID != o.TransferID {
					t.Errorf("TransferID = %s, want %s", d.TransferID, o.TransferID)
				}
				if d.ParallelConnections != o.ParallelConnections {
					t.Errorf("ParallelConnections = %d, want %d", d.ParallelConnections, o.ParallelConnections)
				}
			},
		},
		{
			name:    "TransferQueued",
			msgType: TypeTransferQueued,
			payload: TransferQueued{
				ManifestID:     "manifest123",
				ReceiverPeerID: "receiver1",
				Position:       2,
				Active:         1,
				Max:            4,
			},
			decodeTo: func() any { return &TransferQueued{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*TransferQueued)
				o := original.(TransferQueued)
				if d.ManifestID != o.ManifestID {
					t.Errorf("ManifestID = %s, want %s", d.ManifestID, o.ManifestID)
				}
				if d.ReceiverPeerID != o.ReceiverPeerID {
					t.Errorf("ReceiverPeerID = %s, want %s", d.ReceiverPeerID, o.ReceiverPeerID)
				}
				if d.Position != o.Position {
					t.Errorf("Position = %d, want %d", d.Position, o.Position)
				}
				if d.Active != o.Active {
					t.Errorf("Active = %d, want %d", d.Active, o.Active)
				}
				if d.Max != o.Max {
					t.Errorf("Max = %d, want %d", d.Max, o.Max)
				}
			},
		},
		{
			name:    "IceCandidates",
			msgType: TypeIceCandidates,
			payload: IceCandidates{
				Candidates: []string{"cand1", "cand2"},
			},
			decodeTo: func() any { return &IceCandidates{} },
			verify: func(t *testing.T, decoded, original any) {
				d := decoded.(*IceCandidates)
				o := original.(IceCandidates)
				if len(d.Candidates) != len(o.Candidates) {
					t.Fatalf("Candidates length = %d, want %d", len(d.Candidates), len(o.Candidates))
				}
				for i, cand := range d.Candidates {
					if cand != o.Candidates[i] {
						t.Errorf("Candidates[%d] = %s, want %s", i, cand, o.Candidates[i])
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msgID := NewMsgID()

			// Create envelope
			env, err := NewEnvelope(tt.msgType, msgID, tt.payload)
			if err != nil {
				t.Fatalf("NewEnvelope() error = %v", err)
			}

			// Validate basic envelope
			if err := env.ValidateBasic(); err != nil {
				t.Fatalf("ValidateBasic() error = %v", err)
			}

			// Marshal to JSON
			data, err := json.Marshal(env)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}

			// Unmarshal back
			var decodedEnv Envelope
			if err := json.Unmarshal(data, &decodedEnv); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}

			// Validate decoded envelope
			if err := decodedEnv.ValidateBasic(); err != nil {
				t.Fatalf("ValidateBasic() after unmarshal error = %v", err)
			}

			// Verify envelope fields
			if decodedEnv.V != env.V {
				t.Errorf("unmarshal V = %d, want %d", decodedEnv.V, env.V)
			}
			if decodedEnv.Type != env.Type {
				t.Errorf("unmarshal Type = %s, want %s", decodedEnv.Type, env.Type)
			}
			if decodedEnv.MsgID != env.MsgID {
				t.Errorf("unmarshal MsgID = %s, want %s", decodedEnv.MsgID, env.MsgID)
			}

			// Decode payload
			decodedPayload := tt.decodeTo()
			if err := decodedEnv.DecodePayload(decodedPayload); err != nil {
				t.Fatalf("DecodePayload() error = %v", err)
			}

			// Verify payload fields
			if tt.verify != nil {
				tt.verify(t, decodedPayload, tt.payload)
			}
		})
	}
}
