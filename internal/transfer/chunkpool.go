package transfer

import (
	"sync"

	"github.com/sheerbytes/sheerbytes/internal/bufpool"
)

var chunkPools sync.Map // map[uint32]*bufpool.Pool

func chunkPoolFor(chunkSize uint32) *bufpool.Pool {
	if chunkSize == 0 {
		return nil
	}
	if pool, ok := chunkPools.Load(chunkSize); ok {
		return pool.(*bufpool.Pool)
	}
	pool := bufpool.New(int(chunkSize))
	actual, _ := chunkPools.LoadOrStore(chunkSize, pool)
	return actual.(*bufpool.Pool)
}
