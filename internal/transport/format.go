package transport

import "fmt"

func FormatBytesMiB(n int) string {
	if n <= 0 {
		return "0B"
	}
	const mib = 1024 * 1024
	if n%mib == 0 {
		return fmt.Sprintf("%dMiB", n/mib)
	}
	return fmt.Sprintf("%dB", n)
}

func FormatBytesGiB(n int64) string {
	if n <= 0 {
		return "0.00 GiB"
	}
	const gib = 1024 * 1024 * 1024
	return fmt.Sprintf("%.2f GiB", float64(n)/float64(gib))
}
