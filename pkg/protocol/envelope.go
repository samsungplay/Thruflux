package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const ProtocolVersion = 1

// Envelope wraps all protocol messages with metadata.
type Envelope struct {
	V         int             `json:"v"`
	Type      string          `json:"type"`
	MsgID     string          `json:"msg_id"`
	SessionID string          `json:"session_id,omitempty"`
	From      string          `json:"from,omitempty"`
	To        string          `json:"to,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// NewEnvelope creates a new envelope with the given message type, message ID, and payload.
// The payload is automatically marshaled to JSON.
func NewEnvelope(msgType, msgID string, payload any) (Envelope, error) {
	var rawPayload json.RawMessage
	var err error

	if payload != nil {
		rawPayload, err = json.Marshal(payload)
		if err != nil {
			return Envelope{}, fmt.Errorf("marshal payload: %w", err)
		}
	}

	return Envelope{
		V:       ProtocolVersion,
		Type:    msgType,
		MsgID:   msgID,
		Payload: rawPayload,
	}, nil
}

// DecodePayload unmarshals the envelope's payload into the provided output struct.
func (e Envelope) DecodePayload(out any) error {
	if len(e.Payload) == 0 {
		return errors.New("payload is empty")
	}
	if err := json.Unmarshal(e.Payload, out); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}
	return nil
}

// ValidateBasic performs basic validation on the envelope.
// Returns an error if validation fails.
func (e Envelope) ValidateBasic() error {
	if e.V != ProtocolVersion {
		return fmt.Errorf("invalid protocol version: got %d, expected %d", e.V, ProtocolVersion)
	}
	if e.Type == "" {
		return errors.New("type is required")
	}
	if e.MsgID == "" {
		return errors.New("msg_id is required")
	}
	return nil
}

// NewMsgID generates a random 16-character hex string for message identification.
func NewMsgID() string {
	b := make([]byte, 8) // 8 bytes = 16 hex characters
	if _, err := rand.Read(b); err != nil {
		// Fallback if rand fails (should be extremely rare)
		return "0000000000000000"
	}
	return hex.EncodeToString(b)
}

