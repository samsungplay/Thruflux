package main

import (
	"os"

	"github.com/sheerbytes/sheerbytes/internal/cli/sender"
)

func main() {
	sender.Run(os.Args[1:])
}
