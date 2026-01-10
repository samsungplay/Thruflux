package transfer

import "fmt"

// Bitmap is a compact bitset for tracking chunk presence.
type Bitmap struct {
	bits int
	data []byte
}

// NewBitmap allocates a bitmap sized for the given number of bits.
func NewBitmap(bits int) *Bitmap {
	if bits < 0 {
		bits = 0
	}
	byteLen := (bits + 7) / 8
	return &Bitmap{
		bits: bits,
		data: make([]byte, byteLen),
	}
}

// BitmapFromBytes creates a bitmap using the provided bytes and bit length.
func BitmapFromBytes(data []byte, bits int) (*Bitmap, error) {
	if bits < 0 {
		return nil, fmt.Errorf("invalid bitmap length %d", bits)
	}
	byteLen := (bits + 7) / 8
	if len(data) != byteLen {
		return nil, fmt.Errorf("bitmap length mismatch: got %d, want %d", len(data), byteLen)
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	return &Bitmap{bits: bits, data: buf}, nil
}

// LenBits returns the number of bits in the bitmap.
func (b *Bitmap) LenBits() int {
	if b == nil {
		return 0
	}
	return b.bits
}

// Set marks the bit at index i.
func (b *Bitmap) Set(i int) {
	if b == nil || i < 0 || i >= b.bits {
		return
	}
	byteIndex := i / 8
	bitIndex := uint(i % 8)
	b.data[byteIndex] |= 1 << bitIndex
}

// Get reports whether the bit at index i is set.
func (b *Bitmap) Get(i int) bool {
	if b == nil || i < 0 || i >= b.bits {
		return false
	}
	byteIndex := i / 8
	bitIndex := uint(i % 8)
	return (b.data[byteIndex] & (1 << bitIndex)) != 0
}

// CountSet returns the number of set bits in the bitmap.
func (b *Bitmap) CountSet() int {
	if b == nil {
		return 0
	}
	count := 0
	for _, v := range b.data {
		for v != 0 {
			v &= v - 1
			count++
		}
	}
	return count
}

// HighestSetBit returns the highest set bit index.
func (b *Bitmap) HighestSetBit() (int, bool) {
	if b == nil || b.bits == 0 {
		return -1, false
	}
	for i := b.bits - 1; i >= 0; i-- {
		if b.Get(i) {
			return i, true
		}
	}
	return -1, false
}

// Marshal returns a copy of the bitmap bytes.
func (b *Bitmap) Marshal() []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b.data))
	copy(out, b.data)
	return out
}

// Unmarshal replaces bitmap contents using the provided bytes and bit length.
func (b *Bitmap) Unmarshal(data []byte, bits int) error {
	if bits < 0 {
		return fmt.Errorf("invalid bitmap length %d", bits)
	}
	byteLen := (bits + 7) / 8
	if len(data) != byteLen {
		return fmt.Errorf("bitmap length mismatch: got %d, want %d", len(data), byteLen)
	}
	b.bits = bits
	if cap(b.data) < len(data) {
		b.data = make([]byte, len(data))
	} else {
		b.data = b.data[:len(data)]
	}
	copy(b.data, data)
	return nil
}
