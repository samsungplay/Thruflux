package quictransport

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"time"

	"github.com/quic-go/quic-go"
)

const (
	// ALPNProtocol is the Application-Layer Protocol Negotiation identifier for Thruflux QUIC.
	ALPNProtocol = "thruflux-quic-v1"
)

// ServerConfig returns a TLS configuration for QUIC server.
// Uses self-signed certificate for now (insecure).
func ServerConfig() *tls.Config {
	// Generate a self-signed certificate
	cert, err := generateSelfSignedCert()
	if err != nil {
		panic("failed to generate self-signed certificate: " + err.Error())
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{ALPNProtocol},
	}
}

// ClientConfig returns a TLS configuration for QUIC client.
// Uses InsecureSkipVerify for now (insecure).
func ClientConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{ALPNProtocol},
	}
}

// DefaultServerQUICConfig returns the default QUIC server config.
func DefaultServerQUICConfig() *quic.Config {
	return &quic.Config{
		KeepAlivePeriod:                10 * time.Second,
		MaxIdleTimeout:                 30 * time.Second,
		DisablePathMTUDiscovery:        true,
		MaxIncomingStreams:             100,
		InitialConnectionReceiveWindow: 64 * 1024 * 1024,
		MaxConnectionReceiveWindow:     64 * 1024 * 1024,
		InitialStreamReceiveWindow:     16 * 1024 * 1024,
		MaxStreamReceiveWindow:         16 * 1024 * 1024,
	}
}

// DefaultClientQUICConfig returns the default QUIC client config.
func DefaultClientQUICConfig() *quic.Config {
	return &quic.Config{
		KeepAlivePeriod:                10 * time.Second,
		MaxIdleTimeout:                 30 * time.Second,
		DisablePathMTUDiscovery:        true,
		InitialConnectionReceiveWindow: 64 * 1024 * 1024,
		MaxConnectionReceiveWindow:     64 * 1024 * 1024,
		InitialStreamReceiveWindow:     16 * 1024 * 1024,
		MaxStreamReceiveWindow:         16 * 1024 * 1024,
	}
}

// generateSelfSignedCert generates a self-signed certificate for testing.
func generateSelfSignedCert() (tls.Certificate, error) {
	// Generate RSA private key
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Thruflux"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{""},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour), // Valid for 1 year
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Create self-signed certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
	}, nil
}

// Listen creates a QUIC listener on the given PacketConn.
func Listen(ctx context.Context, udpConn net.PacketConn, logger *slog.Logger) (*quic.Listener, error) {
	return ListenWithConfig(ctx, udpConn, logger, nil)
}

// ListenWithConfig creates a QUIC listener on the given PacketConn using a custom config.
func ListenWithConfig(ctx context.Context, udpConn net.PacketConn, logger *slog.Logger, config *quic.Config) (*quic.Listener, error) {
	fmt.Fprintf(os.Stderr, "[TRACE quic] ListenWithConfig called, localAddr=%v\n", udpConn.LocalAddr())
	tlsConfig := ServerConfig()
	if config == nil {
		config = DefaultServerQUICConfig()
	}

	fmt.Fprintf(os.Stderr, "[TRACE quic] calling quic.Listen...\n")
	listener, err := quic.Listen(udpConn, tlsConfig, config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[TRACE quic] quic.Listen FAILED: %v\n", err)
		logger.Error("QUIC listen failed", "error", err, "local_addr", udpConn.LocalAddr())
		return nil, err
	}

	fmt.Fprintf(os.Stderr, "[TRACE quic] quic.Listen returned OK\n")
	logger.Info("QUIC listener created", "local_addr", udpConn.LocalAddr())
	return listener, nil
}

// Dial creates a QUIC connection to the remote address using the given PacketConn.
func Dial(ctx context.Context, udpConn net.PacketConn, remoteAddr net.Addr, logger *slog.Logger) (*quic.Conn, error) {
	return DialWithConfig(ctx, udpConn, remoteAddr, logger, nil)
}

// DialWithConfig creates a QUIC connection to the remote address using a custom config.
func DialWithConfig(ctx context.Context, udpConn net.PacketConn, remoteAddr net.Addr, logger *slog.Logger, config *quic.Config) (*quic.Conn, error) {
	tlsConfig := ClientConfig()
	if config == nil {
		config = DefaultClientQUICConfig()
	}

	logger.Info("QUIC dial starting", "remote_addr", remoteAddr, "local_addr", udpConn.LocalAddr())

	conn, err := quic.Dial(ctx, udpConn, remoteAddr, tlsConfig, config)
	if err != nil {
		logger.Error("QUIC dial failed", "error", err, "remote_addr", remoteAddr)
		return nil, err
	}

	logger.Info("QUIC connection established", "remote_addr", remoteAddr)
	return conn, nil
}
