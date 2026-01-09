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

	controlTypeFileBegin = byte(0x10)
	controlTypeAck2      = byte(0x11)
	controlTypeFileEnd   = byte(0x12)
	controlTypeFileDone  = byte(0x13)
	controlTypeEnd       = byte(0xFF)
)

type FileBegin struct {
	RelPath   string
	FileSize  uint64
	ChunkSize uint32
	StreamID  uint64
}

type Ack2 struct {
	StreamID               uint64
	HighestContiguousChunk uint32
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

func writeControlHeader(s Stream, m manifest.Manifest) error {
	if _, err := s.Write([]byte(controlMagic)); err != nil {
		return fmt.Errorf("failed to write control magic: %w", err)
	}

	manifestJSON, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	manifestJSONLen := uint32(len(manifestJSON))
	if err := binary.Write(s, binary.BigEndian, manifestJSONLen); err != nil {
		return fmt.Errorf("failed to write manifest length: %w", err)
	}
	if _, err := s.Write(manifestJSON); err != nil {
		return fmt.Errorf("failed to write manifest json: %w", err)
	}

	return nil
}

func readControlHeader(s Stream) (manifest.Manifest, error) {
	var m manifest.Manifest

	magicBuf := make([]byte, len(controlMagic))
	if _, err := io.ReadFull(s, magicBuf); err != nil {
		return m, fmt.Errorf("failed to read control magic: %w", err)
	}
	if string(magicBuf) != controlMagic {
		return m, ErrInvalidManifestMagic
	}

	var manifestJSONLen uint32
	if err := binary.Read(s, binary.BigEndian, &manifestJSONLen); err != nil {
		return m, fmt.Errorf("failed to read manifest length: %w", err)
	}

	manifestJSON := make([]byte, manifestJSONLen)
	if _, err := io.ReadFull(s, manifestJSON); err != nil {
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

	if _, err := s.Write([]byte{controlTypeFileBegin}); err != nil {
		return fmt.Errorf("failed to write FileBegin type: %w", err)
	}

	relPathBytes := []byte(msg.RelPath)
	relPathLen := uint16(len(relPathBytes))
	if err := binary.Write(s, binary.BigEndian, relPathLen); err != nil {
		return fmt.Errorf("failed to write relpath length: %w", err)
	}
	if _, err := s.Write(relPathBytes); err != nil {
		return fmt.Errorf("failed to write relpath: %w", err)
	}

	if err := binary.Write(s, binary.BigEndian, msg.FileSize); err != nil {
		return fmt.Errorf("failed to write file size: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.ChunkSize); err != nil {
		return fmt.Errorf("failed to write chunk size: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.StreamID); err != nil {
		return fmt.Errorf("failed to write stream id: %w", err)
	}

	return nil
}

func readFileBegin(s Stream) (FileBegin, error) {
	var msg FileBegin

	relPath, err := readRelPath(s)
	if err != nil {
		return msg, fmt.Errorf("failed to read relpath: %w", err)
	}
	msg.RelPath = relPath

	if err := binary.Read(s, binary.BigEndian, &msg.FileSize); err != nil {
		return msg, fmt.Errorf("failed to read file size: %w", err)
	}
	if err := binary.Read(s, binary.BigEndian, &msg.ChunkSize); err != nil {
		return msg, fmt.Errorf("failed to read chunk size: %w", err)
	}
	if err := binary.Read(s, binary.BigEndian, &msg.StreamID); err != nil {
		return msg, fmt.Errorf("failed to read stream id: %w", err)
	}

	return msg, nil
}

func writeAck2(s Stream, msg Ack2) error {
	if _, err := s.Write([]byte{controlTypeAck2}); err != nil {
		return fmt.Errorf("failed to write Ack2 type: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.StreamID); err != nil {
		return fmt.Errorf("failed to write stream id: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.HighestContiguousChunk); err != nil {
		return fmt.Errorf("failed to write ack index: %w", err)
	}

	return nil
}

func readAck2(s Stream) (Ack2, error) {
	var msg Ack2
	if err := binary.Read(s, binary.BigEndian, &msg.StreamID); err != nil {
		return msg, fmt.Errorf("failed to read stream id: %w", err)
	}
	if err := binary.Read(s, binary.BigEndian, &msg.HighestContiguousChunk); err != nil {
		return msg, fmt.Errorf("failed to read ack index: %w", err)
	}
	return msg, nil
}

func writeFileEnd(s Stream, msg FileEnd) error {
	if _, err := s.Write([]byte{controlTypeFileEnd}); err != nil {
		return fmt.Errorf("failed to write FileEnd type: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.StreamID); err != nil {
		return fmt.Errorf("failed to write stream id: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.CRC32); err != nil {
		return fmt.Errorf("failed to write crc32: %w", err)
	}
	return nil
}

func readFileEnd(s Stream) (FileEnd, error) {
	var msg FileEnd
	if err := binary.Read(s, binary.BigEndian, &msg.StreamID); err != nil {
		return msg, fmt.Errorf("failed to read stream id: %w", err)
	}
	if err := binary.Read(s, binary.BigEndian, &msg.CRC32); err != nil {
		return msg, fmt.Errorf("failed to read crc32: %w", err)
	}
	return msg, nil
}

func writeFileDone(s Stream, msg FileDone) error {
	if _, err := s.Write([]byte{controlTypeFileDone}); err != nil {
		return fmt.Errorf("failed to write FileDone type: %w", err)
	}
	if err := binary.Write(s, binary.BigEndian, msg.StreamID); err != nil {
		return fmt.Errorf("failed to write stream id: %w", err)
	}
	okByte := byte(0)
	if msg.OK {
		okByte = 1
	}
	if _, err := s.Write([]byte{okByte}); err != nil {
		return fmt.Errorf("failed to write ok byte: %w", err)
	}
	errMsg := []byte(msg.ErrMsg)
	errLen := uint16(len(errMsg))
	if err := binary.Write(s, binary.BigEndian, errLen); err != nil {
		return fmt.Errorf("failed to write err length: %w", err)
	}
	if errLen > 0 {
		if _, err := s.Write(errMsg); err != nil {
			return fmt.Errorf("failed to write err msg: %w", err)
		}
	}
	return nil
}

func readFileDone(s Stream) (FileDone, error) {
	var msg FileDone
	if err := binary.Read(s, binary.BigEndian, &msg.StreamID); err != nil {
		return msg, fmt.Errorf("failed to read stream id: %w", err)
	}
	okBuf := make([]byte, 1)
	if _, err := io.ReadFull(s, okBuf); err != nil {
		return msg, fmt.Errorf("failed to read ok byte: %w", err)
	}
	msg.OK = okBuf[0] == 1

	var errLen uint16
	if err := binary.Read(s, binary.BigEndian, &errLen); err != nil {
		return msg, fmt.Errorf("failed to read err length: %w", err)
	}
	if errLen > 0 {
		errMsg := make([]byte, errLen)
		if _, err := io.ReadFull(s, errMsg); err != nil {
			return msg, fmt.Errorf("failed to read err msg: %w", err)
		}
		msg.ErrMsg = string(errMsg)
	}

	return msg, nil
}

func writeControlEnd(s Stream) error {
	if _, err := s.Write([]byte{controlTypeEnd}); err != nil {
		return fmt.Errorf("failed to write End type: %w", err)
	}
	return nil
}

func readControlMessage(s Stream) (byte, any, error) {
	msgType := make([]byte, 1)
	if _, err := io.ReadFull(s, msgType); err != nil {
		return 0, nil, err
	}

	switch msgType[0] {
	case controlTypeFileBegin:
		msg, err := readFileBegin(s)
		return controlTypeFileBegin, msg, err
	case controlTypeAck2:
		msg, err := readAck2(s)
		return controlTypeAck2, msg, err
	case controlTypeFileEnd:
		msg, err := readFileEnd(s)
		return controlTypeFileEnd, msg, err
	case controlTypeFileDone:
		msg, err := readFileDone(s)
		return controlTypeFileDone, msg, err
	case controlTypeEnd:
		return controlTypeEnd, nil, nil
	default:
		return msgType[0], nil, ErrInvalidRecordType
	}
}
