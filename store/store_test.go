package store

import (
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSetGetDel(t *testing.T) {
	s := New()
	s.Set("a", "1", SetOptions{})
	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q, %v", v, ok)
	}
	if n := s.Del("a", "missing"); n != 1 {
		t.Fatalf("Del = %d, want 1", n)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("key still present after Del")
	}
}

func TestSetNXXX(t *testing.T) {
	s := New()
	if s.Set("k", "v", SetOptions{XX: true}) {
		t.Fatal("XX should fail on missing key")
	}
	if !s.Set("k", "v", SetOptions{NX: true}) {
		t.Fatal("NX should succeed on missing key")
	}
	if s.Set("k", "v2", SetOptions{NX: true}) {
		t.Fatal("NX should fail on existing key")
	}
	if !s.Set("k", "v3", SetOptions{XX: true}) {
		t.Fatal("XX should succeed on existing key")
	}
}

func TestExpiry(t *testing.T) {
	s := New()
	s.Set("tmp", "x", SetOptions{TTL: 30 * time.Millisecond})
	if s.TTL("tmp") <= 0 {
		t.Fatal("expected positive TTL")
	}
	time.Sleep(50 * time.Millisecond)
	if _, ok := s.Get("tmp"); ok {
		t.Fatal("key should have expired")
	}
	if s.TTL("tmp") != -2 {
		t.Fatal("expired key should report TTL -2")
	}
}

func TestPersist(t *testing.T) {
	s := New()
	s.Set("k", "v", SetOptions{TTL: time.Minute})
	if !s.Persist("k") {
		t.Fatal("Persist should succeed")
	}
	if s.TTL("k") != -1 {
		t.Fatal("TTL should be -1 after Persist")
	}
}

func TestIncrBy(t *testing.T) {
	s := New()
	if n, _ := s.IncrBy("c", 5); n != 5 {
		t.Fatalf("got %d", n)
	}
	if n, _ := s.IncrBy("c", -2); n != 3 {
		t.Fatalf("got %d", n)
	}
	s.Set("str", "abc", SetOptions{})
	if _, err := s.IncrBy("str", 1); err != ErrNotInteger {
		t.Fatalf("expected ErrNotInteger, got %v", err)
	}
	s.Set("big", "9223372036854775807", SetOptions{})
	if _, err := s.IncrBy("big", 1); err != ErrOverflow {
		t.Fatalf("expected ErrOverflow, got %v", err)
	}
}

func TestKeysPattern(t *testing.T) {
	s := New()
	for _, k := range []string{"user:1", "user:2", "session:1"} {
		s.Set(k, "x", SetOptions{})
	}
	got := s.Keys("user:*")
	sort.Strings(got)
	if len(got) != 2 || got[0] != "user:1" || got[1] != "user:2" {
		t.Fatalf("Keys = %v", got)
	}
}

func TestJanitorEvicts(t *testing.T) {
	s := New()
	s.StartJanitor(10 * time.Millisecond)
	defer s.StopJanitor()

	s.Set("gone", "x", SetOptions{TTL: 5 * time.Millisecond})
	time.Sleep(40 * time.Millisecond)

	s.mu.RLock()
	_, present := s.data["gone"]
	s.mu.RUnlock()
	if present {
		t.Fatal("janitor did not evict expired key")
	}
}

// Run with -race: many goroutines hammering the same counter must
// never lose an increment.
func TestConcurrentIncr(t *testing.T) {
	s := New()
	const workers, perWorker = 50, 200

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				if _, err := s.IncrBy("counter", 1); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()

	v, _ := s.Get("counter")
	if v != strconv.Itoa(workers*perWorker) {
		t.Fatalf("counter = %s, want %d", v, workers*perWorker)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				s.Set("k"+strconv.Itoa(j%10), strconv.Itoa(i), SetOptions{})
			}
		}(i)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				s.Get("k" + strconv.Itoa(j%10))
				s.Keys("*")
			}
		}()
	}
	wg.Wait()
}
