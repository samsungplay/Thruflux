package quictransport

import (
	"testing"
)

func TestServerConfig(t *testing.T) {
	config := ServerConfig()
	if config == nil {
		t.Fatal("ServerConfig returned nil")
	}

	if len(config.Certificates) == 0 {
		t.Fatal("ServerConfig has no certificates")
	}

	if len(config.NextProtos) == 0 {
		t.Fatal("ServerConfig has no NextProtos")
	}

	found := false
	for _, proto := range config.NextProtos {
		if proto == ALPNProtocol {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("ServerConfig NextProtos does not contain %s", ALPNProtocol)
	}
}

func TestClientConfig(t *testing.T) {
	config := ClientConfig()
	if config == nil {
		t.Fatal("ClientConfig returned nil")
	}

	if !config.InsecureSkipVerify {
		t.Error("ClientConfig InsecureSkipVerify should be true")
	}

	if len(config.NextProtos) == 0 {
		t.Fatal("ClientConfig has no NextProtos")
	}

	found := false
	for _, proto := range config.NextProtos {
		if proto == ALPNProtocol {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("ClientConfig NextProtos does not contain %s", ALPNProtocol)
	}
}

func TestServerConfigCertificates(t *testing.T) {
	config := ServerConfig()
	
	// Verify certificate is valid
	cert := config.Certificates[0]
	if cert.PrivateKey == nil {
		t.Error("Certificate has no private key")
	}

	if len(cert.Certificate) == 0 {
		t.Error("Certificate has no certificate bytes")
	}
}

