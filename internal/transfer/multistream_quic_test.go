package transfer_test

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/quictransport"
	"github.com/sheerbytes/sheerbytes/internal/transfer"
	"github.com/sheerbytes/sheerbytes/internal/transferquic"
	"github.com/sheerbytes/sheerbytes/pkg/manifest"
)

func TestSendRecvManifestMultiStream_QUIC_UnusedStreamsDontBlock(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "multistream_quic_src_*")
	if err != nil {
		t.Fatalf("MkdirTemp src: %v", err)
	}
	defer os.RemoveAll(srcDir)

	const fileSize = 1 * 1024 * 1024
	data := make([]byte, fileSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	filePath := filepath.Join(srcDir, "file.bin")
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	m, err := manifest.Scan(srcDir)
	if err != nil {
		t.Fatalf("manifest.Scan: %v", err)
	}

	outDir, err := os.MkdirTemp("", "multistream_quic_out_*")
	if err != nil {
		t.Fatalf("MkdirTemp out: %v", err)
	}
	defer os.RemoveAll(outDir)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))

	serverUDP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP server: %v", err)
	}
	defer serverUDP.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	listener, err := quictransport.ListenWithConfig(ctx, serverUDP, logger, nil)
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}
	defer listener.Close()

	serverTransport := transferquic.NewListener(listener, logger)
	defer serverTransport.Close()

	clientUDP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP client: %v", err)
	}
	defer clientUDP.Close()

	remoteAddr := serverUDP.LocalAddr()
	quicConn, err := quictransport.DialWithConfig(ctx, clientUDP, remoteAddr, logger, nil)
	if err != nil {
		t.Fatalf("quic dial: %v", err)
	}
	defer quicConn.CloseWithError(0, "")

	clientTransport := transferquic.NewDialer(quicConn, logger)
	defer clientTransport.Close()

	const parallelStreams = 8
	const chunkSize = 256 * 1024 // 1 MiB file => 4 chunks, less than parallelStreams.

	recvErrCh := make(chan error, 1)
	go func() {
		conn, err := serverTransport.Accept(ctx)
		if err != nil {
			recvErrCh <- err
			return
		}
		defer conn.Close()

		_, err = transfer.RecvManifestMultiStream(ctx, conn, outDir, transfer.Options{
			ParallelFiles: parallelStreams,
			Resume:        false,
		})
		recvErrCh <- err
	}()

	conn, err := clientTransport.Dial(ctx, "peer")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	sendErr := transfer.SendManifestMultiStream(ctx, conn, srcDir, m, transfer.Options{
		ParallelFiles: parallelStreams,
		ChunkSize:     chunkSize,
		Resume:        false,
	})
	recvErr := <-recvErrCh
	if sendErr != nil {
		t.Fatalf("SendManifestMultiStream: %v", sendErr)
	}
	if recvErr != nil {
		t.Fatalf("RecvManifestMultiStream: %v", recvErr)
	}

	rootOut := filepath.Join(outDir, m.Root)
	got, err := os.ReadFile(filepath.Join(rootOut, "file.bin"))
	if err != nil {
		t.Fatalf("ReadFile out: %v", err)
	}
	if !reflect.DeepEqual(got, data) {
		t.Fatalf("file content mismatch: got=%d bytes want=%d bytes", len(got), len(data))
	}
}
