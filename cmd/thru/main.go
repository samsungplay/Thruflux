package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/cli/receiver"
	"github.com/sheerbytes/sheerbytes/internal/cli/sender"
	"github.com/sheerbytes/sheerbytes/internal/termio"
)

const (
	version = "v0.1.1"
	banner  = `
████████╗██╗  ██╗██████╗ ██╗   ██╗███████╗██╗     ██╗   ██╗██╗  ██╗
╚══██╔══╝██║  ██║██╔══██╗██║   ██║██╔════╝██║     ██║   ██║╚██╗██╔╝
   ██║   ███████║██████╔╝██║   ██║█████╗  ██║     ██║   ██║ ╚███╔╝ 
   ██║   ██╔══██║██╔══██╗██║   ██║██╔══╝  ██║     ██║   ██║ ██╔██╗ 
   ██║   ██║  ██║██║  ██║╚██████╔╝██║     ███████╗╚██████╔╝██╔╝ ██╗
   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═╝     ╚══════╝ ╚═════╝ ╚═╝  ╚═╝
Thruflux v0.1.1
High Performance P2P File Transfer
Pure Throughput. Zero Friction.
Made with passion by @infiniteplay
May your NAT be kind and your paths be direct!
`
)

var (
	startupMessages = []string{
		"May your P2P be direct.",
		"May your packets avoid relays.",
		"May NAT be gentle with you.",
		"May your hops be few.",
		"Throughput incoming.",
		"Engaging direct transfer.",
		"Bypassing the middlemen.",
		"Seeking the shortest path.",
		"Opening the fast lane.",
		"Preparing pure throughput.",
		"Less relay, more speed.",
		"Optimizing your packets’ life choices.",
		"Convincing NATs to cooperate.",
		"Negotiating with the network gods.",
		"Hoping for IPv6 enlightenment.",
		"Warming up the transport engines.",
		"Avoiding TURN like a pro.",
		"May your sockets stay open.",
		"No relays were harmed in this transfer.",
		"Taking the scenic-free route.",
		"Sending packets with purpose.",
		"Direct is better.",
		"Trusting QUIC with your bytes.",
		"Attempting maximum throughput.",
		"Straight line transfer engaged.",
		"Routing around nonsense.",
		"Fast path requested.",
		"No middlemen today.",
		"Minimal hops, maximal hope.",
		"Let’s move some bytes.",
	}
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func main() {
	termio.Init()
	args := os.Args[1:]
	if len(args) == 0 {
		printBanner()
		printUsage()
		return
	}
	if hasVersionFlag(args) {
		printBanner()
		return
	}

	cmdName := args[0]
	switch cmdName {
	case "host":
		if shouldPrintStartupMessage(args[1:]) {
			msg := pickStartupMessage()
			if isTTY(termio.StderrFile()) {
				_ = os.Setenv("THRU_STARTUP_MESSAGE", msg)
			} else {
				fmt.Fprintf(termio.Stdout(), ">>.. %s\n", msg)
			}
		}
		sender.Run(args[1:])
		return
	case "join":
		if shouldPrintStartupMessage(args[1:]) {
			msg := pickStartupMessage()
			if isTTY(termio.StderrFile()) {
				_ = os.Setenv("THRU_STARTUP_MESSAGE", msg)
			} else {
				fmt.Fprintf(termio.Stdout(), ">>.. %s\n", msg)
			}
		}
		receiver.Run(args[1:])
		return
	default:
		if hasHelpFlag(args) {
			printUsage()
			return
		}
		fmt.Fprintf(termio.Stderr(), "unknown command: %s\n", cmdName)
		printUsage()
		os.Exit(2)
	}
}

func printUsage() {
	fmt.Fprintln(termio.Stderr(), "usage: thru <command> [args]")
	fmt.Fprintln(termio.Stderr(), "commands:")
	fmt.Fprintln(termio.Stderr(), "  host some files for others to download")
	fmt.Fprintln(termio.Stderr(), "  join a session and download files")
	fmt.Fprintln(termio.Stderr(), "quick examples:")
	fmt.Fprintln(termio.Stderr(), "  thru host <path>")
	fmt.Fprintln(termio.Stderr(), "  thru host <path1> <path2> <path3>...")
	fmt.Fprintln(termio.Stderr(), "  thru join <join-code> --out ./downloads")
	fmt.Fprintln(termio.Stderr(), "to learn detailed usage:")
	fmt.Fprintln(termio.Stderr(), "  thru host --help")
	fmt.Fprintln(termio.Stderr(), "  thru join --help")
}

func hasHelpFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			return true
		}
	}
	return false
}

func hasVersionFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--version" || arg == "-v" {
			return true
		}
	}
	return false
}

func shouldPrintStartupMessage(args []string) bool {
	if len(args) == 0 {
		return false
	}
	if hasHelpFlag(args) || hasVersionFlag(args) {
		return false
	}
	return true
}

func pickStartupMessage() string {
	if len(startupMessages) == 0 {
		return ""
	}
	return startupMessages[rng.Intn(len(startupMessages))]
}

func printShareSummary(paths []string) {
	if len(paths) == 0 {
		return
	}
	fmt.Fprintln(termio.Stdout(), "Sharing the following paths:")
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}
		if strings.HasPrefix(path, "-") {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			continue
		}
		fmt.Fprintf(termio.Stdout(), "  - %s\n", path)
	}
}

func printBanner() {
	fmt.Fprint(termio.Stdout(), banner)
}

func isTTY(f *os.File) bool {
	if f == nil {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
