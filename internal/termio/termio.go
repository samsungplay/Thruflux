package termio

import (
	"io"
	"os"
	"sync"
)

type writer struct {
	file *os.File
	ch   chan []byte
}

func (w *writer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	buf := make([]byte, len(p))
	copy(buf, p)
	w.ch <- buf
	return len(p), nil
}

func (w *writer) File() *os.File {
	return w.file
}

type manager struct {
	once   sync.Once
	stdout *writer
	stderr *writer
}

var global manager

func Init() {
	global.once.Do(func() {
		global.stdout = newWriter(os.Stdout)
		global.stderr = newWriter(os.Stderr)
	})
}

func newWriter(f *os.File) *writer {
	w := &writer{
		file: f,
		ch:   make(chan []byte, 1024),
	}
	go func() {
		for buf := range w.ch {
			_, _ = w.file.Write(buf)
		}
	}()
	return w
}

func Stdout() io.Writer {
	Init()
	return global.stdout
}

func Stderr() io.Writer {
	Init()
	return global.stderr
}

func StdoutFile() *os.File {
	Init()
	return global.stdout.file
}

func StderrFile() *os.File {
	Init()
	return global.stderr.file
}
