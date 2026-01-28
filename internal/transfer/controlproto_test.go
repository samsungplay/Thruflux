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
		RelPath:      "a/b.txt",
		FileSize:     1234,
		ChunkSize:    4096,
		StreamID:     42,
		HashAlg:      HashAlgCRC32C,
		StripeIndex:  1,
		StripeCount:  4,
		StripeStart:  10,
		StripeChunks: 5,
	}
	credit := Credit{
		StreamID: 42,
		Credits:  9,
	}
	creditBatch := CreditBatch{
		Entries: []Credit{
			{StreamID: 7, Credits: 3},
			{StreamID: 9, Credits: 12},
		},
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
	resumeReq := ResumeRequest{
		FileID:   "abcd1234",
		StreamID: 42,
	}
	resumeInfo := FileResumeInfo{
		FileID:            "abcd1234",
		StreamID:          42,
		TotalChunks:       10,
		Bitmap:            []byte{0xFF, 0x03},
		LastVerifiedChunk: 9,
		LastVerifiedHash:  0xDEADBEEF,
	}
	dataStreams := DataStreams{
		Count: 8,
	}

	if err := writeFileBegin(stream, begin); err != nil {
		t.Fatalf("write FileBegin: %v", err)
	}
	if err := writeCredit(stream, credit); err != nil {
		t.Fatalf("write Credit: %v", err)
	}
	if err := writeCreditBatch(stream, creditBatch); err != nil {
		t.Fatalf("write CreditBatch: %v", err)
	}
	if err := writeFileEnd(stream, end); err != nil {
		t.Fatalf("write FileEnd: %v", err)
	}
	if err := writeFileDone(stream, done); err != nil {
		t.Fatalf("write FileDone: %v", err)
	}
	if err := writeResumeRequest(stream, resumeReq); err != nil {
		t.Fatalf("write ResumeRequest: %v", err)
	}
	if err := writeFileResumeInfo(stream, resumeInfo); err != nil {
		t.Fatalf("write FileResumeInfo: %v", err)
	}
	if err := writeDataStreams(stream, dataStreams); err != nil {
		t.Fatalf("write DataStreams: %v", err)
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
	if err != nil || msgType != controlTypeCredit {
		t.Fatalf("read Credit: type=%v err=%v", msgType, err)
	}
	decodedCredit := msg.(Credit)
	if decodedCredit != credit {
		t.Fatalf("Credit mismatch: got %+v want %+v", decodedCredit, credit)
	}

	msgType, msg, err = readControlMessage(stream)
	if err != nil || msgType != controlTypeCreditBatch {
		t.Fatalf("read CreditBatch: type=%v err=%v", msgType, err)
	}
	decodedBatch := msg.(CreditBatch)
	if len(decodedBatch.Entries) != len(creditBatch.Entries) {
		t.Fatalf("CreditBatch length mismatch: got %d want %d", len(decodedBatch.Entries), len(creditBatch.Entries))
	}
	for i := range decodedBatch.Entries {
		if decodedBatch.Entries[i] != creditBatch.Entries[i] {
			t.Fatalf("CreditBatch mismatch at %d: got %+v want %+v", i, decodedBatch.Entries[i], creditBatch.Entries[i])
		}
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

	msgType, msg, err = readControlMessage(stream)
	if err != nil || msgType != controlTypeResumeRequest {
		t.Fatalf("read ResumeRequest: type=%v err=%v", msgType, err)
	}
	decodedReq := msg.(ResumeRequest)
	if decodedReq != resumeReq {
		t.Fatalf("ResumeRequest mismatch: got %+v want %+v", decodedReq, resumeReq)
	}

	msgType, msg, err = readControlMessage(stream)
	if err != nil || msgType != controlTypeFileResumeInfo {
		t.Fatalf("read FileResumeInfo: type=%v err=%v", msgType, err)
	}
	decodedInfo := msg.(FileResumeInfo)
	if decodedInfo.FileID != resumeInfo.FileID ||
		decodedInfo.StreamID != resumeInfo.StreamID ||
		decodedInfo.TotalChunks != resumeInfo.TotalChunks ||
		decodedInfo.LastVerifiedChunk != resumeInfo.LastVerifiedChunk ||
		decodedInfo.LastVerifiedHash != resumeInfo.LastVerifiedHash ||
		!bytes.Equal(decodedInfo.Bitmap, resumeInfo.Bitmap) {
		t.Fatalf("FileResumeInfo mismatch: got %+v want %+v", decodedInfo, resumeInfo)
	}

	msgType, msg, err = readControlMessage(stream)
	if err != nil || msgType != controlTypeDataStreams {
		t.Fatalf("read DataStreams: type=%v err=%v", msgType, err)
	}
	decodedStreams := msg.(DataStreams)
	if decodedStreams != dataStreams {
		t.Fatalf("DataStreams mismatch: got %+v want %+v", decodedStreams, dataStreams)
	}
}
