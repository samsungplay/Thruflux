package transfer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/sheerbytes/sheerbytes/pkg/manifest"
)

const (
	controlMagic = "SBC1"

	controlTypeFileBegin      = byte(0x10)
	controlTypeCredit         = byte(0x11)
	controlTypeFileEnd        = byte(0x12)
	controlTypeFileDone       = byte(0x13)
	controlTypeFileResumeInfo = byte(0x14)
	controlTypeResumeRequest  = byte(0x15)
	controlTypeCreditBatch    = byte(0x16)
	controlTypeDataStreams    = byte(0x17)
	controlTypeEnd            = byte(0xFF)
)

type FileBegin struct {
	RelPath      string
	FileSize     uint64
	ChunkSize    uint32
	StreamID     uint64
	HashAlg      byte
	StripeIndex  uint16
	StripeCount  uint16
	StripeStart  uint32
	StripeChunks uint32
}

type Credit struct {
	StreamID uint64
	Credits  uint32
}

type CreditBatch struct {
	Entries []Credit
}

type FileEnd struct {
	StreamID uint64
	CRC32    uint32
}

type FileDone struct {
	StreamID uint64
	OK       bool
	ErrMsg   string
}

type FileResumeInfo struct {
	FileID            string
	StreamID          uint64
	TotalChunks       uint32
	Bitmap            []byte
	LastVerifiedChunk uint32
	LastVerifiedHash  uint64
}

type ResumeRequest struct {
	FileID   string
	StreamID uint64
}

type DataStreams struct {
	Count uint16
}

func writeControlHeader(s Stream, m manifest.Manifest) error {
	if err := writeFullControl(s, []byte(controlMagic), "control magic"); err != nil {
		return fmt.Errorf("failed to write control magic: %w", err)
	}

	manifestJSON, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	manifestJSONLen := uint32(len(manifestJSON))
	if err := writeUint32Control(s, manifestJSONLen, "manifest length"); err != nil {
		return fmt.Errorf("failed to write manifest length: %w", err)
	}
	if err := writeFullControl(s, manifestJSON, "manifest json"); err != nil {
		return fmt.Errorf("failed to write manifest json: %w", err)
	}

	return nil
}

func readControlHeader(s Stream) (manifest.Manifest, error) {
	var m manifest.Manifest

	magicBuf := make([]byte, len(controlMagic))
	if err := readFullControl(s, magicBuf, "control magic"); err != nil {
		return m, fmt.Errorf("failed to read control magic: %w", err)
	}
	if string(magicBuf) != controlMagic {
		return m, ErrInvalidManifestMagic
	}

	manifestJSONLen, err := readUint32Control(s, "manifest length")
	if err != nil {
		return m, fmt.Errorf("failed to read manifest length: %w", err)
	}

	manifestJSON := make([]byte, manifestJSONLen)
	if err := readFullControl(s, manifestJSON, "manifest json"); err != nil {
		return m, fmt.Errorf("failed to read manifest json: %w", err)
	}
	if err := json.Unmarshal(manifestJSON, &m); err != nil {
		return m, fmt.Errorf("failed to unmarshal manifest: %w", err)
	}

	return m, nil
}

func writeFileBegin(s Stream, msg FileBegin) error {
	if err := validateRelPath(msg.RelPath); err != nil {
		return err
	}

	if err := writeFullControl(s, []byte{controlTypeFileBegin}, "file begin type"); err != nil {
		return fmt.Errorf("failed to write FileBegin type: %w", err)
	}

	relPathBytes := []byte(msg.RelPath)
	relPathLen := uint16(len(relPathBytes))
	if err := writeUint16Control(s, relPathLen, "relpath length"); err != nil {
		return fmt.Errorf("failed to write relpath length: %w", err)
	}
	if err := writeFullControl(s, relPathBytes, "relpath"); err != nil {
		return fmt.Errorf("failed to write relpath: %w", err)
	}

	if err := writeUint64Control(s, msg.FileSize, "file size"); err != nil {
		return fmt.Errorf("failed to write file size: %w", err)
	}
	if err := writeUint32Control(s, msg.ChunkSize, "chunk size"); err != nil {
		return fmt.Errorf("failed to write chunk size: %w", err)
	}
	if err := writeUint64Control(s, msg.StreamID, "stream id"); err != nil {
		return fmt.Errorf("failed to write stream id: %w", err)
	}
	if err := writeFullControl(s, []byte{msg.HashAlg}, "hash alg"); err != nil {
		return fmt.Errorf("failed to write hash alg: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.StripeIndex); err != nil {
		return fmt.Errorf("failed to write stripe index: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.StripeCount); err != nil {
		return fmt.Errorf("failed to write stripe count: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.StripeStart); err != nil {
		return fmt.Errorf("failed to write stripe start: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.StripeChunks); err != nil {
		return fmt.Errorf("failed to write stripe chunks: %w", err)
	}

	return nil
}

func readFileBegin(s Stream) (FileBegin, error) {
	var msg FileBegin

	relPath, err := readRelPathControl(s)
	if err != nil {
		return msg, fmt.Errorf("failed to read relpath: %w", err)
	}
	msg.RelPath = relPath

	fileSize, err := readUint64Control(s, "file size")
	if err != nil {
		return msg, fmt.Errorf("failed to read file size: %w", err)
	}
	msg.FileSize = fileSize
	chunkSize, err := readUint32Control(s, "chunk size")
	if err != nil {
		return msg, fmt.Errorf("failed to read chunk size: %w", err)
	}
	msg.ChunkSize = chunkSize
	streamID, err := readUint64Control(s, "stream id")
	if err != nil {
		return msg, fmt.Errorf("failed to read stream id: %w", err)
	}
	msg.StreamID = streamID
	hashBuf := make([]byte, 1)
	if err := readFullControl(s, hashBuf, "hash alg"); err != nil {
		return msg, fmt.Errorf("failed to read hash alg: %w", err)
	}
	msg.HashAlg = hashBuf[0]
	if err := binary.Read(s, binary.BigEndian, &msg.StripeIndex); err != nil {
		return msg, fmt.Errorf("failed to read stripe index: %w", err)
	}
	if err := binary.Read(s, binary.BigEndian, &msg.StripeCount); err != nil {
		return msg, fmt.Errorf("failed to read stripe count: %w", err)
	}
	if err := binary.Read(s, binary.BigEndian, &msg.StripeStart); err != nil {
		return msg, fmt.Errorf("failed to read stripe start: %w", err)
	}
	if err := binary.Read(s, binary.BigEndian, &msg.StripeChunks); err != nil {
		return msg, fmt.Errorf("failed to read stripe chunks: %w", err)
	}

	return msg, nil
}

func writeCredit(s Stream, msg Credit) error {
	if _, err := s.Write([]byte{controlTypeCredit}); err != nil {
		return fmt.Errorf("failed to write Credit type: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.StreamID); err != nil {
		return fmt.Errorf("failed to write stream id: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.Credits); err != nil {
		return fmt.Errorf("failed to write credit count: %w", err)
	}

	return nil
}

func readCredit(s Stream) (Credit, error) {
	var msg Credit
	if err := binary.Read(s, binary.BigEndian, &msg.StreamID); err != nil {
		return msg, fmt.Errorf("failed to read stream id: %w", err)
	}
	if err := binary.Read(s, binary.BigEndian, &msg.Credits); err != nil {
		return msg, fmt.Errorf("failed to read credit count: %w", err)
	}
	return msg, nil
}

func writeCreditBatch(s Stream, batch CreditBatch) error {
	if _, err := s.Write([]byte{controlTypeCreditBatch}); err != nil {
		return fmt.Errorf("failed to write CreditBatch type: %w", err)
	}
	count := uint32(len(batch.Entries))
	if err := binary.Write(s, binary.BigEndian, count); err != nil {
		return fmt.Errorf("failed to write CreditBatch count: %w", err)
	}
	for _, entry := range batch.Entries {
		if err := binary.Write(s, binary.BigEndian, entry.StreamID); err != nil {
			return fmt.Errorf("failed to write CreditBatch stream id: %w", err)
		}
		if err := binary.Write(s, binary.BigEndian, entry.Credits); err != nil {
			return fmt.Errorf("failed to write CreditBatch credits: %w", err)
		}
	}
	return nil
}

func readCreditBatch(s Stream) (CreditBatch, error) {
	var batch CreditBatch
	var count uint32
	if err := binary.Read(s, binary.BigEndian, &count); err != nil {
		return batch, fmt.Errorf("failed to read CreditBatch count: %w", err)
	}
	if count == 0 {
		batch.Entries = nil
		return batch, nil
	}
	entries := make([]Credit, count)
	for i := uint32(0); i < count; i++ {
		if err := binary.Read(s, binary.BigEndian, &entries[i].StreamID); err != nil {
			return batch, fmt.Errorf("failed to read CreditBatch stream id: %w", err)
		}
		if err := binary.Read(s, binary.BigEndian, &entries[i].Credits); err != nil {
			return batch, fmt.Errorf("failed to read CreditBatch credits: %w", err)
		}
	}
	batch.Entries = entries
	return batch, nil
}

func writeFileEnd(s Stream, msg FileEnd) error {
	if err := writeFullControl(s, []byte{controlTypeFileEnd}, "file end type"); err != nil {
		return fmt.Errorf("failed to write FileEnd type: %w", err)
	}
	if err := writeUint64Control(s, msg.StreamID, "stream id"); err != nil {
		return fmt.Errorf("failed to write stream id: %w", err)
	}
	if err := writeUint32Control(s, msg.CRC32, "crc32"); err != nil {
		return fmt.Errorf("failed to write crc32: %w", err)
	}
	return nil
}

func readFileEnd(s Stream) (FileEnd, error) {
	var msg FileEnd
	streamID, err := readUint64Control(s, "stream id")
	if err != nil {
		return msg, fmt.Errorf("failed to read stream id: %w", err)
	}
	msg.StreamID = streamID
	crc32Value, err := readUint32Control(s, "crc32")
	if err != nil {
		return msg, fmt.Errorf("failed to read crc32: %w", err)
	}
	msg.CRC32 = crc32Value
	return msg, nil
}

func writeFileDone(s Stream, msg FileDone) error {
	if err := writeFullControl(s, []byte{controlTypeFileDone}, "file done type"); err != nil {
		return fmt.Errorf("failed to write FileDone type: %w", err)
	}
	if err := writeUint64Control(s, msg.StreamID, "stream id"); err != nil {
		return fmt.Errorf("failed to write stream id: %w", err)
	}
	okByte := byte(0)
	if msg.OK {
		okByte = 1
	}
	if err := writeFullControl(s, []byte{okByte}, "ok byte"); err != nil {
		return fmt.Errorf("failed to write ok byte: %w", err)
	}
	errMsg := []byte(msg.ErrMsg)
	errLen := uint16(len(errMsg))
	if err := writeUint16Control(s, errLen, "err length"); err != nil {
		return fmt.Errorf("failed to write err length: %w", err)
	}
	if errLen > 0 {
		if err := writeFullControl(s, errMsg, "err msg"); err != nil {
			return fmt.Errorf("failed to write err msg: %w", err)
		}
	}
	return nil
}

func readFileDone(s Stream) (FileDone, error) {
	var msg FileDone
	streamID, err := readUint64Control(s, "stream id")
	if err != nil {
		return msg, fmt.Errorf("failed to read stream id: %w", err)
	}
	msg.StreamID = streamID
	okBuf := make([]byte, 1)
	if err := readFullControl(s, okBuf, "ok byte"); err != nil {
		return msg, fmt.Errorf("failed to read ok byte: %w", err)
	}
	msg.OK = okBuf[0] == 1

	errLen, err := readUint16Control(s, "err length")
	if err != nil {
		return msg, fmt.Errorf("failed to read err length: %w", err)
	}
	if errLen > 0 {
		errMsg := make([]byte, errLen)
		if err := readFullControl(s, errMsg, "err msg"); err != nil {
			return msg, fmt.Errorf("failed to read err msg: %w", err)
		}
		msg.ErrMsg = string(errMsg)
	}

	return msg, nil
}

func writeFileResumeInfo(s Stream, msg FileResumeInfo) error {
	if err := writeFullControl(s, []byte{controlTypeFileResumeInfo}, "file resume info type"); err != nil {
		return fmt.Errorf("failed to write FileResumeInfo type: %w", err)
	}
	fileIDBytes := []byte(msg.FileID)
	fileIDLen := uint16(len(fileIDBytes))
	if err := writeUint16Control(s, fileIDLen, "file id length"); err != nil {
		return fmt.Errorf("failed to write file id length: %w", err)
	}
	if err := writeFullControl(s, fileIDBytes, "file id"); err != nil {
		return fmt.Errorf("failed to write file id: %w", err)
	}
	if err := writeUint64Control(s, msg.StreamID, "stream id"); err != nil {
		return fmt.Errorf("failed to write stream id: %w", err)
	}
	if err := writeUint32Control(s, msg.TotalChunks, "total chunks"); err != nil {
		return fmt.Errorf("failed to write total chunks: %w", err)
	}
	bitmapLen := uint32(len(msg.Bitmap))
	if err := writeUint32Control(s, bitmapLen, "bitmap length"); err != nil {
		return fmt.Errorf("failed to write bitmap length: %w", err)
	}
	if bitmapLen > 0 {
		if err := writeFullControl(s, msg.Bitmap, "bitmap"); err != nil {
			return fmt.Errorf("failed to write bitmap: %w", err)
		}
	}
	if err := writeUint32Control(s, msg.LastVerifiedChunk, "last verified chunk"); err != nil {
		return fmt.Errorf("failed to write last verified chunk: %w", err)
	}
	if err := writeUint64Control(s, msg.LastVerifiedHash, "last verified hash"); err != nil {
		return fmt.Errorf("failed to write last verified hash: %w", err)
	}
	return nil
}

func readFileResumeInfo(s Stream) (FileResumeInfo, error) {
	var msg FileResumeInfo
	fileIDLen, err := readUint16Control(s, "file id length")
	if err != nil {
		return msg, fmt.Errorf("failed to read file id length: %w", err)
	}
	if fileIDLen > 0 {
		fileID := make([]byte, fileIDLen)
		if err := readFullControl(s, fileID, "file id"); err != nil {
			return msg, fmt.Errorf("failed to read file id: %w", err)
		}
		msg.FileID = string(fileID)
	}
	streamID, err := readUint64Control(s, "stream id")
	if err != nil {
		return msg, fmt.Errorf("failed to read stream id: %w", err)
	}
	msg.StreamID = streamID
	totalChunks, err := readUint32Control(s, "total chunks")
	if err != nil {
		return msg, fmt.Errorf("failed to read total chunks: %w", err)
	}
	msg.TotalChunks = totalChunks
	bitmapLen, err := readUint32Control(s, "bitmap length")
	if err != nil {
		return msg, fmt.Errorf("failed to read bitmap length: %w", err)
	}
	if bitmapLen > 0 {
		msg.Bitmap = make([]byte, bitmapLen)
		if err := readFullControl(s, msg.Bitmap, "bitmap"); err != nil {
			return msg, fmt.Errorf("failed to read bitmap: %w", err)
		}
	}
	lastVerifiedChunk, err := readUint32Control(s, "last verified chunk")
	if err != nil {
		return msg, fmt.Errorf("failed to read last verified chunk: %w", err)
	}
	msg.LastVerifiedChunk = lastVerifiedChunk
	lastVerifiedHash, err := readUint64Control(s, "last verified hash")
	if err != nil {
		return msg, fmt.Errorf("failed to read last verified hash: %w", err)
	}
	msg.LastVerifiedHash = lastVerifiedHash
	return msg, nil
}

func writeResumeRequest(s Stream, msg ResumeRequest) error {
	if err := writeFullControl(s, []byte{controlTypeResumeRequest}, "resume request type"); err != nil {
		return fmt.Errorf("failed to write ResumeRequest type: %w", err)
	}
	fileIDBytes := []byte(msg.FileID)
	fileIDLen := uint16(len(fileIDBytes))
	if err := writeUint16Control(s, fileIDLen, "file id length"); err != nil {
		return fmt.Errorf("failed to write file id length: %w", err)
	}
	if err := writeFullControl(s, fileIDBytes, "file id"); err != nil {
		return fmt.Errorf("failed to write file id: %w", err)
	}
	if err := writeUint64Control(s, msg.StreamID, "stream id"); err != nil {
		return fmt.Errorf("failed to write stream id: %w", err)
	}
	return nil
}

func readResumeRequest(s Stream) (ResumeRequest, error) {
	var msg ResumeRequest
	fileIDLen, err := readUint16Control(s, "file id length")
	if err != nil {
		return msg, fmt.Errorf("failed to read file id length: %w", err)
	}
	if fileIDLen > 0 {
		fileID := make([]byte, fileIDLen)
		if err := readFullControl(s, fileID, "file id"); err != nil {
			return msg, fmt.Errorf("failed to read file id: %w", err)
		}
		msg.FileID = string(fileID)
	}
	streamID, err := readUint64Control(s, "stream id")
	if err != nil {
		return msg, fmt.Errorf("failed to read stream id: %w", err)
	}
	msg.StreamID = streamID
	return msg, nil
}

func writeDataStreams(s Stream, msg DataStreams) error {
	if err := writeFullControl(s, []byte{controlTypeDataStreams}, "data streams type"); err != nil {
		return fmt.Errorf("failed to write DataStreams type: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.Count); err != nil {
		return fmt.Errorf("failed to write data streams count: %w", err)
	}
	return nil
}

func readDataStreams(s Stream) (DataStreams, error) {
	var msg DataStreams
	if err := binary.Read(s, binary.BigEndian, &msg.Count); err != nil {
		return msg, fmt.Errorf("failed to read data streams count: %w", err)
	}
	return msg, nil
}

func writeControlEnd(s Stream) error {
	if err := writeFullControl(s, []byte{controlTypeEnd}, "control end"); err != nil {
		return fmt.Errorf("failed to write End type: %w", err)
	}
	return nil
}

func readControlMessage(s Stream) (byte, any, error) {
	msgType := make([]byte, 1)
	if err := readFullControl(s, msgType, "control type"); err != nil {
		return 0, nil, err
	}

	switch msgType[0] {
	case controlTypeFileBegin:
		msg, err := readFileBegin(s)
		return controlTypeFileBegin, msg, err
	case controlTypeCredit:
		msg, err := readCredit(s)
		return controlTypeCredit, msg, err
	case controlTypeCreditBatch:
		msg, err := readCreditBatch(s)
		return controlTypeCreditBatch, msg, err
	case controlTypeFileEnd:
		msg, err := readFileEnd(s)
		return controlTypeFileEnd, msg, err
	case controlTypeFileDone:
		msg, err := readFileDone(s)
		return controlTypeFileDone, msg, err
	case controlTypeFileResumeInfo:
		msg, err := readFileResumeInfo(s)
		return controlTypeFileResumeInfo, msg, err
	case controlTypeResumeRequest:
		msg, err := readResumeRequest(s)
		return controlTypeResumeRequest, msg, err
	case controlTypeDataStreams:
		msg, err := readDataStreams(s)
		return controlTypeDataStreams, msg, err
	case controlTypeEnd:
		return controlTypeEnd, nil, nil
	default:
		return msgType[0], nil, ErrInvalidRecordType
	}
}

func readRelPathControl(s Stream) (string, error) {
	relPathLen, err := readUint16Control(s, "relpath length")
	if err != nil {
		return "", err
	}
	if relPathLen > maxRelPathLength {
		return "", ErrRelPathTooLong
	}
	relPathBuf := make([]byte, relPathLen)
	if relPathLen > 0 {
		if err := readFullControl(s, relPathBuf, "relpath"); err != nil {
			return "", err
		}
	}
	return string(relPathBuf), nil
}

func readFullControl(s Stream, buf []byte, op string) error {
	_, err := io.ReadFull(s, buf)
	if err != nil {
		return fmt.Errorf("control stream read %s: %w", op, err)
	}
	return nil
}

func writeFullControl(s Stream, buf []byte, op string) error {
	written := 0
	for written < len(buf) {
		n, err := s.Write(buf[written:])
		if err != nil {
			return fmt.Errorf("control stream write %s: %w", op, err)
		}
		written += n
	}
	return nil
}

func readUint16Control(s Stream, op string) (uint16, error) {
	var buf [2]byte
	if err := readFullControl(s, buf[:], op); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(buf[:]), nil
}

func readUint32Control(s Stream, op string) (uint32, error) {
	var buf [4]byte
	if err := readFullControl(s, buf[:], op); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

func readUint64Control(s Stream, op string) (uint64, error) {
	var buf [8]byte
	if err := readFullControl(s, buf[:], op); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func writeUint16Control(s Stream, value uint16, op string) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], value)
	return writeFullControl(s, buf[:], op)
}

func writeUint32Control(s Stream, value uint32, op string) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)
	return writeFullControl(s, buf[:], op)
}

func writeUint64Control(s Stream, value uint64, op string) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return writeFullControl(s, buf[:], op)
}
