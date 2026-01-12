package main

import (
	"os"

	"github.com/sheerbytes/sheerbytes/internal/cli/receiver"
)

func main() {
	receiver.Run(os.Args[1:])
}
