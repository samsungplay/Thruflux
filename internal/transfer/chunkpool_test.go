package transfer

import "testing"

func TestChunkPoolReuse(t *testing.T) {
	poolA := chunkPoolFor(1024)
	poolB := chunkPoolFor(1024)
	if poolA != poolB {
		t.Fatalf("expected same pool for identical chunk sizes")
	}

	buf := poolA.Get()
	if len(buf) != 1024 {
		t.Fatalf("expected buffer size 1024, got %d", len(buf))
	}
	poolA.Put(buf)
}
