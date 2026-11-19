package aof

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/iwdie/Redis-Clone/resp"
)

func TestAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")

	a, err := Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	a.Append("SET", "name", "naveen")
	a.Append("INCR", "visits")
	a.Append("DEL", "name")
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	var got [][]string
	n, err := Replay(path, func(args []resp.Value) {
		row := make([]string, len(args))
		for i, v := range args {
			row[i] = v.Str
		}
		got = append(got, row)
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || got[0][2] != "naveen" || got[2][0] != "DEL" {
		t.Fatalf("replayed %d: %v", n, got)
	}
}

func TestReplayMissingFile(t *testing.T) {
	n, err := Replay(filepath.Join(t.TempDir(), "nope.aof"), func([]resp.Value) {})
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}
