package transfer

import "testing"

func TestBitmapBasics(t *testing.T) {
	b := NewBitmap(10)
	if b.LenBits() != 10 {
		t.Fatalf("LenBits mismatch: got %d", b.LenBits())
	}
	b.Set(0)
	b.Set(3)
	b.Set(9)

	if !b.Get(0) || !b.Get(3) || !b.Get(9) {
		t.Fatalf("expected bits to be set")
	}
	if b.Get(1) || b.Get(8) {
		t.Fatalf("unexpected bits set")
	}

	if count := b.CountSet(); count != 3 {
		t.Fatalf("CountSet mismatch: got %d", count)
	}

	highest, ok := b.HighestSetBit()
	if !ok || highest != 9 {
		t.Fatalf("HighestSetBit mismatch: got %d ok=%v", highest, ok)
	}
}

func TestBitmapMarshalUnmarshal(t *testing.T) {
	b := NewBitmap(9)
	b.Set(0)
	b.Set(4)
	b.Set(8)

	data := b.Marshal()
	clone, err := BitmapFromBytes(data, 9)
	if err != nil {
		t.Fatalf("BitmapFromBytes: %v", err)
	}

	if clone.CountSet() != 3 || !clone.Get(0) || !clone.Get(4) || !clone.Get(8) {
		t.Fatalf("bitmap round-trip mismatch")
	}

	var empty Bitmap
	if err := empty.Unmarshal(data, 9); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if empty.CountSet() != 3 || !empty.Get(0) || !empty.Get(4) || !empty.Get(8) {
		t.Fatalf("bitmap unmarshal mismatch")
	}
}
