package app

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/sheerbytes/sheerbytes/internal/transfer"
)

const (
	authLabel       = "thruflux-auth-v1"
	authVersion     = byte(1)
	authRoleSender  = byte(1)
	authRoleReceive = byte(2)
	authNonceSize   = 16
	authMacSize     = 32
	authMsgSize     = 1 + 1 + authNonceSize + authMacSize
)

type authExporter interface {
	ExportKeyingMaterial(label string, context []byte, length int) ([]byte, error)
}

func authenticateTransport(ctx context.Context, conn transfer.Conn, joinCode string, role byte) error {
	key, err := deriveAuthKey(conn, joinCode)
	if err != nil {
		return err
	}

	switch role {
	case authRoleSender:
		return authAsSender(ctx, conn, key)
	case authRoleReceive:
		return authAsReceiver(ctx, conn, key)
	default:
		return fmt.Errorf("invalid auth role: %d", role)
	}
}

func authAsSender(ctx context.Context, conn transfer.Conn, key []byte) error {
	stream, err := conn.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("auth open stream: %w", err)
	}
	defer stream.Close()

	nonce, err := randomNonce()
	if err != nil {
		return err
	}
	mac := computeAuthMac(key, authRoleSender, nonce)
	if err := writeAuthMessage(ctx, stream, authRoleSender, nonce, mac); err != nil {
		return err
	}

	role, rnonce, rmac, err := readAuthMessage(ctx, stream)
	if err != nil {
		return err
	}
	if role != authRoleReceive {
		return fmt.Errorf("auth role mismatch: expected receiver, got %d", role)
	}
	expected := computeAuthMac(key, role, rnonce)
	if !hmac.Equal(rmac, expected) {
		return fmt.Errorf("auth proof mismatch (receiver)")
	}
	return nil
}

func authAsReceiver(ctx context.Context, conn transfer.Conn, key []byte) error {
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return fmt.Errorf("auth accept stream: %w", err)
	}
	defer stream.Close()

	role, nonce, mac, err := readAuthMessage(ctx, stream)
	if err != nil {
		return err
	}
	if role != authRoleSender {
		return fmt.Errorf("auth role mismatch: expected sender, got %d", role)
	}
	expected := computeAuthMac(key, role, nonce)
	if !hmac.Equal(mac, expected) {
		return fmt.Errorf("auth proof mismatch (sender)")
	}

	respNonce, err := randomNonce()
	if err != nil {
		return err
	}
	respMac := computeAuthMac(key, authRoleReceive, respNonce)
	if err := writeAuthMessage(ctx, stream, authRoleReceive, respNonce, respMac); err != nil {
		return err
	}
	return nil
}

func deriveAuthKey(conn transfer.Conn, joinCode string) ([]byte, error) {
	exporter, ok := conn.(authExporter)
	if !ok {
		return nil, fmt.Errorf("transport does not support key export")
	}
	ekm, err := exporter.ExportKeyingMaterial(authLabel, nil, authMacSize)
	if err != nil {
		return nil, fmt.Errorf("export keying material: %w", err)
	}
	mac := hmac.New(sha256.New, []byte(joinCode))
	_, _ = mac.Write(ekm)
	return mac.Sum(nil), nil
}

func computeAuthMac(key []byte, role byte, nonce []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte{authVersion, role})
	_, _ = mac.Write(nonce)
	return mac.Sum(nil)
}

func randomNonce() ([]byte, error) {
	nonce := make([]byte, authNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("auth nonce: %w", err)
	}
	return nonce, nil
}

func writeAuthMessage(ctx context.Context, w io.Writer, role byte, nonce []byte, mac []byte) error {
	if len(nonce) != authNonceSize {
		return fmt.Errorf("auth nonce length %d", len(nonce))
	}
	if len(mac) != authMacSize {
		return fmt.Errorf("auth mac length %d", len(mac))
	}
	buf := make([]byte, authMsgSize)
	buf[0] = authVersion
	buf[1] = role
	copy(buf[2:], nonce)
	copy(buf[2+authNonceSize:], mac)
	return writeWithContext(ctx, w, buf)
}

func readAuthMessage(ctx context.Context, r io.Reader) (byte, []byte, []byte, error) {
	buf := make([]byte, authMsgSize)
	if err := readWithContext(ctx, r, buf); err != nil {
		return 0, nil, nil, err
	}
	if buf[0] != authVersion {
		return 0, nil, nil, fmt.Errorf("unexpected auth version %d", buf[0])
	}
	role := buf[1]
	nonce := make([]byte, authNonceSize)
	mac := make([]byte, authMacSize)
	copy(nonce, buf[2:2+authNonceSize])
	copy(mac, buf[2+authNonceSize:])
	return role, nonce, mac, nil
}

func readWithContext(ctx context.Context, r io.Reader, buf []byte) error {
	errCh := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(r, buf)
		errCh <- err
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("auth read: %w", err)
		}
		return nil
	}
}

func writeWithContext(ctx context.Context, w io.Writer, buf []byte) error {
	errCh := make(chan error, 1)
	go func() {
		_, err := w.Write(buf)
		errCh <- err
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("auth write: %w", err)
		}
		return nil
	}
}
