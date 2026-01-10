package transfer

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	HashAlgNone     = byte(0)
	HashAlgCRC32C   = byte(1)
	HashAlgXXHash64 = byte(2)
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func parseHashAlg(name string) (byte, error) {
	switch name {
	case "", "crc32c":
		return HashAlgCRC32C, nil
	case "none":
		return HashAlgNone, nil
	case "xxhash64":
		return HashAlgXXHash64, nil
	default:
		return 0, fmt.Errorf("unknown hash algorithm %q", name)
	}
}

func hashChunk(alg byte, data []byte) (uint64, error) {
	switch alg {
	case HashAlgNone:
		return 0, nil
	case HashAlgCRC32C:
		sum := crc32.Checksum(data, crc32cTable)
		return uint64(sum), nil
	case HashAlgXXHash64:
		return xxhash64(data), nil
	default:
		return 0, fmt.Errorf("unsupported hash algorithm %d", alg)
	}
}

// Minimal xxHash64 implementation for resume verification.
func xxhash64(b []byte) uint64 {
	prime1 := uint64(11400714785074694791)
	prime2 := uint64(14029467366897019727)
	prime3 := uint64(1609587929392839161)
	prime4 := uint64(9650029242287828579)
	prime5 := uint64(2870177450012600261)

	n := len(b)
	var h uint64
	if n >= 32 {
		v1 := prime1 + prime2
		v2 := prime2
		v3 := uint64(0)
		v4 := ^prime1

		for len(b) >= 32 {
			v1 = xxround(v1, binary.LittleEndian.Uint64(b[0:8]))
			v2 = xxround(v2, binary.LittleEndian.Uint64(b[8:16]))
			v3 = xxround(v3, binary.LittleEndian.Uint64(b[16:24]))
			v4 = xxround(v4, binary.LittleEndian.Uint64(b[24:32]))
			b = b[32:]
		}

		h = rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18)
		h = xxmerge(h, v1)
		h = xxmerge(h, v2)
		h = xxmerge(h, v3)
		h = xxmerge(h, v4)
	} else {
		h = prime5
	}

	h += uint64(n)

	for len(b) >= 8 {
		k1 := xxround(0, binary.LittleEndian.Uint64(b[:8]))
		h ^= k1
		h = rotl(h, 27)*prime1 + prime4
		b = b[8:]
	}

	if len(b) >= 4 {
		h ^= uint64(binary.LittleEndian.Uint32(b[:4])) * prime1
		h = rotl(h, 23)*prime2 + prime3
		b = b[4:]
	}

	for _, c := range b {
		h ^= uint64(c) * prime5
		h = rotl(h, 11) * prime1
	}

	return xxavalanche(h)
}

func xxround(acc, input uint64) uint64 {
	const (
		prime1 uint64 = 11400714785074694791
		prime2 uint64 = 14029467366897019727
	)
	acc += input * prime2
	acc = rotl(acc, 31)
	acc *= prime1
	return acc
}

func xxmerge(acc, val uint64) uint64 {
	const (
		prime1 uint64 = 11400714785074694791
		prime4 uint64 = 9650029242287828579
	)
	acc ^= xxround(0, val)
	acc = acc*prime1 + prime4
	return acc
}

func xxavalanche(h uint64) uint64 {
	const (
		prime2 uint64 = 14029467366897019727
		prime3 uint64 = 1609587929392839161
	)
	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32
	return h
}

func rotl(x uint64, r uint) uint64 {
	return (x << r) | (x >> (64 - r))
}
