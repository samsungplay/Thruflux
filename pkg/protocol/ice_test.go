package protocol

import (
	"encoding/json"
	"testing"
)

func TestIceCredentials_RoundTrip(t *testing.T) {
	// Create IceCredentials
	creds := IceCredentials{
		Ufrag: "test-ufrag-123",
		Pwd:   "test-password-456",
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(creds)
	if err != nil {
		t.Fatalf("failed to marshal IceCredentials: %v", err)
	}

	// Unmarshal from JSON
	var decodedCreds IceCredentials
	if err := json.Unmarshal(jsonData, &decodedCreds); err != nil {
		t.Fatalf("failed to unmarshal IceCredentials: %v", err)
	}

	// Verify round-trip
	if decodedCreds.Ufrag != creds.Ufrag {
		t.Errorf("Ufrag mismatch: got %s, want %s", decodedCreds.Ufrag, creds.Ufrag)
	}
	if decodedCreds.Pwd != creds.Pwd {
		t.Errorf("Pwd mismatch: got %s, want %s", decodedCreds.Pwd, creds.Pwd)
	}
}

func TestIceCredentials_Envelope(t *testing.T) {
	creds := IceCredentials{
		Ufrag: "test-ufrag",
		Pwd:   "test-pwd",
	}

	// Create envelope
	env, err := NewEnvelope(TypeIceCredentials, NewMsgID(), creds)
	if err != nil {
		t.Fatalf("failed to create envelope: %v", err)
	}

	// Verify envelope type
	if env.Type != TypeIceCredentials {
		t.Errorf("envelope type = %s, want %s", env.Type, TypeIceCredentials)
	}

	// Decode payload
	var decodedCreds IceCredentials
	if err := env.DecodePayload(&decodedCreds); err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}

	// Verify payload
	if decodedCreds.Ufrag != creds.Ufrag {
		t.Errorf("Ufrag mismatch: got %s, want %s", decodedCreds.Ufrag, creds.Ufrag)
	}
	if decodedCreds.Pwd != creds.Pwd {
		t.Errorf("Pwd mismatch: got %s, want %s", decodedCreds.Pwd, creds.Pwd)
	}
}

