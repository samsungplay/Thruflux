package transport

import "testing"

func TestFormatBytesMiB(t *testing.T) {
	if got := FormatBytesMiB(0); got != "0B" {
		t.Fatalf("expected 0B, got %q", got)
	}
	if got := FormatBytesMiB(8 * 1024 * 1024); got != "8MiB" {
		t.Fatalf("expected 8MiB, got %q", got)
	}
	if got := FormatBytesMiB(1024); got != "1024B" {
		t.Fatalf("expected raw bytes, got %q", got)
	}
}

func TestFormatBytesGiB(t *testing.T) {
	if got := FormatBytesGiB(0); got != "0.00 GiB" {
		t.Fatalf("expected 0.00 GiB, got %q", got)
	}
	if got := FormatBytesGiB(1 * 1024 * 1024 * 1024); got != "1.00 GiB" {
		t.Fatalf("expected 1.00 GiB, got %q", got)
	}
}
