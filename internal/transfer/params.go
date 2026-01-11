package transfer

// RuntimeParams are the effective per-file transfer settings.
type RuntimeParams struct {
	ChunkSize     uint32
	WindowSize    uint32
	ReadAhead     uint32
	ParallelFiles int
}

// NormalizeParams applies defaults and clamps runtime parameters.
func NormalizeParams(p RuntimeParams, opts Options) RuntimeParams {
	out := p
	if out.ChunkSize == 0 {
		out.ChunkSize = opts.ChunkSize
	}
	if out.ChunkSize == 0 {
		out.ChunkSize = DefaultChunkSize
	}
	if out.WindowSize == 0 {
		out.WindowSize = opts.WindowSize
	}
	if out.WindowSize == 0 {
		out.WindowSize = DefaultWindowSize
	}
	if out.ReadAhead == 0 {
		out.ReadAhead = opts.ReadAhead
	}
	if out.ReadAhead == 0 {
		out.ReadAhead = out.WindowSize + 4
	}
	if out.ReadAhead < 1 {
		out.ReadAhead = 1
	}
	if out.ReadAhead > 256 {
		out.ReadAhead = 256
	}
	if out.ParallelFiles == 0 {
		out.ParallelFiles = opts.ParallelFiles
	}
	if out.ParallelFiles < 1 {
		out.ParallelFiles = 1
	}
	if out.ParallelFiles > 32 {
		out.ParallelFiles = 32
	}
	return out
}
