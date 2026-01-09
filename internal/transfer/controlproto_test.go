package transfer

import (
	"bytes"
	"testing"
)

type memStream struct {
	buf *bytes.Buffer
}

func (m *memStream) Read(p []byte) (int, error)  { return m.buf.Read(p) }
func (m *memStream) Write(p []byte) (int, error) { return m.buf.Write(p) }
func (m *memStream) Close() error                { return nil }

func TestControlProto_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	stream := &memStream{buf: &buf}

	begin := FileBegin{
		RelPath:   "a/b.txt",
		FileSize:  1234,
		ChunkSize: 4096,
		StreamID:  42,
	}
	ack := Ack2{
		StreamID:               42,
		HighestContiguousChunk: 9,
	}
	end := FileEnd{
		StreamID: 42,
		CRC32:    0xDEADBEEF,
	}
	done := FileDone{
		StreamID: 42,
		OK:       false,
		ErrMsg:   "bad crc",
	}

	if err := writeFileBegin(stream, begin); err != nil {
		t.Fatalf("write FileBegin: %v", err)
	}
	if err := writeAck2(stream, ack); err != nil {
		t.Fatalf("write Ack2: %v", err)
	}
	if err := writeFileEnd(stream, end); err != nil {
		t.Fatalf("write FileEnd: %v", err)
	}
	if err := writeFileDone(stream, done); err != nil {
		t.Fatalf("write FileDone: %v", err)
	}

	msgType, msg, err := readControlMessage(stream)
	if err != nil || msgType != controlTypeFileBegin {
		t.Fatalf("read FileBegin: type=%v err=%v", msgType, err)
	}
	decodedBegin := msg.(FileBegin)
	if decodedBegin != begin {
		t.Fatalf("FileBegin mismatch: got %+v want %+v", decodedBegin, begin)
	}

	msgType, msg, err = readControlMessage(stream)
	if err != nil || msgType != controlTypeAck2 {
		t.Fatalf("read Ack2: type=%v err=%v", msgType, err)
	}
	decodedAck := msg.(Ack2)
	if decodedAck != ack {
		t.Fatalf("Ack2 mismatch: got %+v want %+v", decodedAck, ack)
	}

	msgType, msg, err = readControlMessage(stream)
	if err != nil || msgType != controlTypeFileEnd {
		t.Fatalf("read FileEnd: type=%v err=%v", msgType, err)
	}
	decodedEnd := msg.(FileEnd)
	if decodedEnd != end {
		t.Fatalf("FileEnd mismatch: got %+v want %+v", decodedEnd, end)
	}

	msgType, msg, err = readControlMessage(stream)
	if err != nil || msgType != controlTypeFileDone {
		t.Fatalf("read FileDone: type=%v err=%v", msgType, err)
	}
	decodedDone := msg.(FileDone)
	if decodedDone != done {
		t.Fatalf("FileDone mismatch: got %+v want %+v", decodedDone, done)
	}
}
