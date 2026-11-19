// Package aof implements append-only-file persistence. Write commands are
// sent over a buffered channel to a single writer goroutine, so client
// goroutines never block on disk I/O and the file is only touched from one
// place.
package aof

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/iwdie/Redis-Clone/resp"
)

type AOF struct {
	path string
	f    *os.File

	entries chan []byte
	done    chan struct{}
	once    sync.Once
}

// Open opens (or creates) the AOF at path and starts the writer goroutine.
// Buffered data is flushed and fsynced every syncEvery.
func Open(path string, syncEvery time.Duration) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	a := &AOF{
		path:    path,
		f:       f,
		entries: make(chan []byte, 1024),
		done:    make(chan struct{}),
	}
	go a.writer(syncEvery)
	return a, nil
}

func (a *AOF) writer(syncEvery time.Duration) {
	defer close(a.done)

	w := bufio.NewWriter(a.f)
	ticker := time.NewTicker(syncEvery)
	defer ticker.Stop()

	flush := func() {
		if w.Buffered() == 0 {
			return
		}
		w.Flush()
		a.f.Sync()
	}

	for {
		select {
		case b, ok := <-a.entries:
			if !ok {
				flush()
				return
			}
			w.Write(b)
		case <-ticker.C:
			flush()
		}
	}
}

// Append queues a command for persistence.
func (a *AOF) Append(args ...string) {
	vals := make([]resp.Value, len(args))
	for i, s := range args {
		vals[i] = resp.Bulk(s)
	}
	a.entries <- resp.Arr(vals).Marshal()
}

// Close drains pending writes, syncs and closes the file.
func (a *AOF) Close() error {
	var err error
	a.once.Do(func() {
		close(a.entries)
		<-a.done
		err = a.f.Close()
	})
	return err
}

// Replay reads every command in the file at path and passes it to fn.
// A truncated final command (e.g. after a crash) is ignored.
func Replay(path string, fn func(args []resp.Value)) (int, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := resp.NewReader(f)
	n := 0
	for {
		v, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return n, nil
			}
			return n, err
		}
		if v.Type == resp.Array && len(v.Array) > 0 {
			fn(v.Array)
			n++
		}
	}
}
