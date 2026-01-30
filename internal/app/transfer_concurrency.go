package app

import "github.com/sheerbytes/sheerbytes/internal/transfer"

func computeParallelBudget(fileCount, requestedStreams, connections int, allowStriping bool) (totalStreams int, perConn int) {
	if connections < 1 {
		connections = 1
	}
	totalStreams = requestedStreams
	if totalStreams <= 0 {
		totalStreams = transfer.HeuristicParams(fileCount).ParallelFiles
	}
	if totalStreams < 1 {
		totalStreams = 1
	}
	if connections > 1 && totalStreams < connections {
		totalStreams = connections
	}
	if !allowStriping && fileCount > 0 && totalStreams > fileCount {
		totalStreams = fileCount
	}
	if connections > 1 && totalStreams > 1 {
		maxTotal := connections * 4
		if maxTotal < 2 {
			maxTotal = 2
		}
		if totalStreams > maxTotal {
			totalStreams = maxTotal
		}
		if !allowStriping && fileCount > 0 && totalStreams > fileCount {
			totalStreams = fileCount
		}
	}
	perConn = (totalStreams + connections - 1) / connections
	if perConn < 1 {
		perConn = 1
	}
	return totalStreams, perConn
}
