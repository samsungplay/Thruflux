package transfer

// RuntimeParams are the effective per-file transfer settings.
type RuntimeParams struct {
	ChunkSize     uint32
	ParallelFiles int
}

// HeuristicParams returns deterministic transfer settings based on file count.
func HeuristicParams(fileCount int) RuntimeParams {
	parallel := fileCount
	if parallel < 1 {
		parallel = 1
	}
	if parallel > 6 {
		parallel = 6
	}
	return RuntimeParams{
		ChunkSize:     DefaultChunkSize,
		ParallelFiles: parallel,
	}
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
	if out.ParallelFiles == 0 {
		out.ParallelFiles = opts.ParallelFiles
	}
	if out.ParallelFiles < 1 {
		out.ParallelFiles = 1
	}
	if out.ParallelFiles > 8 {
		out.ParallelFiles = 8
	}
	return out
}
