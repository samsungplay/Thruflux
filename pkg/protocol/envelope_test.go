package protocol

import (
	"encoding/json"
	"testing"
)

func TestNewEnvelope(t *testing.T) {
	tests := []struct {
		name    string
		msgType string
		msgID   string
		payload any
		wantErr bool
	}{
		{
			name:    "Hello message",
			msgType: "hello",
			msgID:   "test123",
			payload: Hello{
				PeerID: "peer1",
				Role:   "sender",
				Capabilities: map[string]any{
					"version": "1.0",
				},
			},
			wantErr: false,
		},
		{
			name:    "Error message",
			msgType: "error",
			msgID:   "test456",
			payload: Error{
				Code:    "INVALID_REQUEST",
				Message: "Invalid request format",
			},
			wantErr: false,
		},
		{
			name:    "PeerList message",
			msgType: "peer_list",
			msgID:   "test789",
			payload: PeerList{
				Peers: []PeerInfo{
					{PeerID: "peer1", Role: "sender"},
					{PeerID: "peer2", Role: "receiver"},
				},
			},
			wantErr: false,
		},
		{
			name:    "nil payload",
			msgType: "test",
			msgID:   "test000",
			payload: nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := NewEnvelope(tt.msgType, tt.msgID, tt.payload)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewEnvelope() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			// Validate basic envelope structure
			if env.V != ProtocolVersion {
				t.Errorf("NewEnvelope() V = %d, want %d", env.V, ProtocolVersion)
			}
			if env.Type != tt.msgType {
				t.Errorf("NewEnvelope() Type = %s, want %s", env.Type, tt.msgType)
			}
			if env.MsgID != tt.msgID {
				t.Errorf("NewEnvelope() MsgID = %s, want %s", env.MsgID, tt.msgID)
			}
		})
	}
}

func TestEnvelope_JSONRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		msgType string
		payload any
		verify  func(t *testing.T, env Envelope, originalPayload any)
	}{
		{
			name:    "Hello round-trip",
			msgType: "hello",
			payload: Hello{
				PeerID: "peer1",
				Role:   "sender",
				Capabilities: map[string]any{
					"version": "1.0",
					"feature": true,
				},
			},
			verify: func(t *testing.T, env Envelope, originalPayload any) {
				var decoded Hello
				if err := env.DecodePayload(&decoded); err != nil {
					t.Fatalf("DecodePayload() error = %v", err)
				}
				original := originalPayload.(Hello)
				if decoded.PeerID != original.PeerID {
					t.Errorf("DecodePayload() PeerID = %s, want %s", decoded.PeerID, original.PeerID)
				}
				if decoded.Role != original.Role {
					t.Errorf("DecodePayload() Role = %s, want %s", decoded.Role, original.Role)
				}
				if len(decoded.Capabilities) != len(original.Capabilities) {
					t.Errorf("DecodePayload() Capabilities length = %d, want %d", len(decoded.Capabilities), len(original.Capabilities))
				}
			},
		},
		{
			name:    "Error round-trip",
			msgType: "error",
			payload: Error{
				Code:    "INVALID_REQUEST",
				Message: "Invalid request format",
			},
			verify: func(t *testing.T, env Envelope, originalPayload any) {
				var decoded Error
				if err := env.DecodePayload(&decoded); err != nil {
					t.Fatalf("DecodePayload() error = %v", err)
				}
				original := originalPayload.(Error)
				if decoded.Code != original.Code {
					t.Errorf("DecodePayload() Code = %s, want %s", decoded.Code, original.Code)
				}
				if decoded.Message != original.Message {
					t.Errorf("DecodePayload() Message = %s, want %s", decoded.Message, original.Message)
				}
			},
		},
		{
			name:    "PeerList round-trip",
			msgType: "peer_list",
			payload: PeerList{
				Peers: []PeerInfo{
					{PeerID: "peer1", Role: "sender"},
					{PeerID: "peer2", Role: "receiver"},
				},
			},
			verify: func(t *testing.T, env Envelope, originalPayload any) {
				var decoded PeerList
				if err := env.DecodePayload(&decoded); err != nil {
					t.Fatalf("DecodePayload() error = %v", err)
				}
				original := originalPayload.(PeerList)
				if len(decoded.Peers) != len(original.Peers) {
					t.Fatalf("DecodePayload() Peers length = %d, want %d", len(decoded.Peers), len(original.Peers))
				}
				for i, peer := range decoded.Peers {
					if peer.PeerID != original.Peers[i].PeerID {
						t.Errorf("DecodePayload() Peers[%d].PeerID = %s, want %s", i, peer.PeerID, original.Peers[i].PeerID)
					}
					if peer.Role != original.Peers[i].Role {
						t.Errorf("DecodePayload() Peers[%d].Role = %s, want %s", i, peer.Role, original.Peers[i].Role)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msgID := NewMsgID()
			original, err := NewEnvelope(tt.msgType, msgID, tt.payload)
			if err != nil {
				t.Fatalf("NewEnvelope() error = %v", err)
			}

			// Validate before marshaling
			if err := original.ValidateBasic(); err != nil {
				t.Fatalf("ValidateBasic() error = %v", err)
			}

			// Marshal to JSON
			data, err := json.Marshal(original)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}

			// Unmarshal back
			var decoded Envelope
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}

			// Validate after unmarshaling
			if err := decoded.ValidateBasic(); err != nil {
				t.Fatalf("ValidateBasic() after unmarshal error = %v", err)
			}

			// Compare envelope fields
			if decoded.V != original.V {
				t.Errorf("unmarshal V = %d, want %d", decoded.V, original.V)
			}
			if decoded.Type != original.Type {
				t.Errorf("unmarshal Type = %s, want %s", decoded.Type, original.Type)
			}
			if decoded.MsgID != original.MsgID {
				t.Errorf("unmarshal MsgID = %s, want %s", decoded.MsgID, original.MsgID)
			}

			// Verify payload decoding
			if tt.verify != nil {
				tt.verify(t, decoded, tt.payload)
			}
		})
	}
}

func TestEnvelope_UnknownFieldsIgnored(t *testing.T) {
	// Create a valid envelope JSON with extra unknown fields
	jsonData := `{
		"v": 1,
		"type": "hello",
		"msg_id": "test123",
		"session_id": "session1",
		"from": "peer1",
		"to": "peer2",
		"unknown_field": "should be ignored",
		"another_unknown": 123,
		"payload": {"peer_id":"peer1","role":"sender"}
	}`

	var env Envelope
	if err := json.Unmarshal([]byte(jsonData), &env); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	// Should still validate correctly
	if err := env.ValidateBasic(); err != nil {
		t.Fatalf("ValidateBasic() error = %v", err)
	}

	// Should decode payload correctly
	var hello Hello
	if err := env.DecodePayload(&hello); err != nil {
		t.Fatalf("DecodePayload() error = %v", err)
	}

	if hello.PeerID != "peer1" {
		t.Errorf("DecodePayload() PeerID = %s, want peer1", hello.PeerID)
	}
	if hello.Role != "sender" {
		t.Errorf("DecodePayload() Role = %s, want sender", hello.Role)
	}
}

func TestEnvelope_ValidateBasic(t *testing.T) {
	tests := []struct {
		name    string
		env     Envelope
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid envelope",
			env: Envelope{
				V:     ProtocolVersion,
				Type:  "hello",
				MsgID: "test123",
			},
			wantErr: false,
		},
		{
			name: "wrong version",
			env: Envelope{
				V:     999,
				Type:  "hello",
				MsgID: "test123",
			},
			wantErr: true,
			errMsg:  "invalid protocol version",
		},
		{
			name: "missing type",
			env: Envelope{
				V:     ProtocolVersion,
				Type:  "",
				MsgID: "test123",
			},
			wantErr: true,
			errMsg:  "type is required",
		},
		{
			name: "missing msg_id",
			env: Envelope{
				V:     ProtocolVersion,
				Type:  "hello",
				MsgID: "",
			},
			wantErr: true,
			errMsg:  "msg_id is required",
		},
		{
			name: "missing both type and msg_id",
			env: Envelope{
				V:     ProtocolVersion,
				Type:  "",
				MsgID: "",
			},
			wantErr: true,
			errMsg:  "type is required", // Should fail on first check
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.env.ValidateBasic()
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBasic() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err != nil {
				if tt.errMsg != "" && err.Error()[:len(tt.errMsg)] != tt.errMsg {
					t.Errorf("ValidateBasic() error = %v, want error containing %s", err, tt.errMsg)
				}
			}
		})
	}
}

func TestNewMsgID(t *testing.T) {
	// Generate multiple IDs to ensure uniqueness
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := NewMsgID()
		if len(id) != 16 {
			t.Errorf("NewMsgID() length = %d, want 16", len(id))
		}
		if ids[id] {
			t.Errorf("NewMsgID() generated duplicate ID: %s", id)
		}
		ids[id] = true
	}
}

func TestEnvelope_OptionalFields(t *testing.T) {
	// Test that optional fields can be set and preserved
	env := Envelope{
		V:         ProtocolVersion,
		Type:      "hello",
		MsgID:     "test123",
		SessionID: "session1",
		From:      "peer1",
		To:        "peer2",
		Payload:   []byte(`{"peer_id":"peer1","role":"sender"}`),
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded Envelope
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.SessionID != env.SessionID {
		t.Errorf("SessionID = %s, want %s", decoded.SessionID, env.SessionID)
	}
	if decoded.From != env.From {
		t.Errorf("From = %s, want %s", decoded.From, env.From)
	}
	if decoded.To != env.To {
		t.Errorf("To = %s, want %s", decoded.To, env.To)
	}
}

