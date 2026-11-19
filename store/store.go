// Package store holds the in-memory keyspace. All access goes through a
// sync.RWMutex so many clients can read at once while writes stay exclusive.
package store

import (
	"errors"
	"path"
	"strconv"
	"sync"
	"time"
)

var (
	ErrNotInteger = errors.New("ERR value is not an integer or out of range")
	ErrOverflow   = errors.New("ERR increment or decrement would overflow")
)

type entry struct {
	value     string
	expiresAt time.Time // zero means no expiry
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// Store is a thread-safe string key-value store with optional TTLs.
type Store struct {
	mu   sync.RWMutex
	data map[string]entry

	stop chan struct{}
	done chan struct{}
}

func New() *Store {
	return &Store{data: make(map[string]entry)}
}

// SetOptions mirrors the flags accepted by SET.
type SetOptions struct {
	TTL time.Duration // 0 = no expiry
	NX  bool          // only set if key does not exist
	XX  bool          // only set if key already exists
}

// Set stores value under key. It reports whether the write happened
// (it may not when NX/XX conditions fail).
func (s *Store) Set(key, value string, opt SetOptions) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	e, ok := s.data[key]
	exists := ok && !e.expired(now)
	if (opt.NX && exists) || (opt.XX && !exists) {
		return false
	}

	ne := entry{value: value}
	if opt.TTL > 0 {
		ne.expiresAt = now.Add(opt.TTL)
	}
	s.data[key] = ne
	return true
}

// Get returns the value for key. Expired keys are treated as missing.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return "", false
	}
	if e.expired(time.Now()) {
		s.deleteIfExpired(key)
		return "", false
	}
	return e.value, true
}

// deleteIfExpired removes key lazily, re-checking under the write lock in
// case another goroutine refreshed it in between.
func (s *Store) deleteIfExpired(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.data[key]; ok && e.expired(time.Now()) {
		delete(s.data, key)
	}
}

// Del removes keys and returns how many existed.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	n := 0
	for _, k := range keys {
		if e, ok := s.data[k]; ok {
			if !e.expired(now) {
				n++
			}
			delete(s.data, k)
		}
	}
	return n
}

// Exists counts how many of keys are present (duplicates count twice,
// like Redis).
func (s *Store) Exists(keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	n := 0
	for _, k := range keys {
		if e, ok := s.data[k]; ok && !e.expired(now) {
			n++
		}
	}
	return n
}

// IncrBy atomically adds delta to the integer stored at key.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	e, ok := s.data[key]
	if ok && e.expired(now) {
		ok = false
		e = entry{}
	}

	var cur int64
	if ok {
		n, err := strconv.ParseInt(e.value, 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		cur = n
	}

	if (delta > 0 && cur > (1<<63-1)-delta) || (delta < 0 && cur < (-1<<63)-delta) {
		return 0, ErrOverflow
	}
	cur += delta
	e.value = strconv.FormatInt(cur, 10)
	s.data[key] = e // keeps the existing TTL, like Redis
	return cur, nil
}

// Append appends value to key and returns the new length.
func (s *Store) Append(key, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if ok && e.expired(time.Now()) {
		e = entry{}
	}
	e.value += value
	s.data[key] = e
	return len(e.value)
}

// Expire sets a TTL on an existing key. Returns false if the key is missing.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	e, ok := s.data[key]
	if !ok || e.expired(now) {
		return false
	}
	if ttl <= 0 {
		delete(s.data, key)
		return true
	}
	e.expiresAt = now.Add(ttl)
	s.data[key] = e
	return true
}

// ExpireAt sets an absolute expiry time. A time in the past deletes the key.
func (s *Store) ExpireAt(key string, at time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	e, ok := s.data[key]
	if !ok || e.expired(now) {
		return false
	}
	if !at.After(now) {
		delete(s.data, key)
		return true
	}
	e.expiresAt = at
	s.data[key] = e
	return true
}

// Persist removes the TTL from key.
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok || e.expired(time.Now()) || e.expiresAt.IsZero() {
		return false
	}
	e.expiresAt = time.Time{}
	s.data[key] = e
	return true
}

// TTL follows Redis semantics: -2 if the key is missing, -1 if it has no
// expiry, otherwise the remaining time.
func (s *Store) TTL(key string) time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	e, ok := s.data[key]
	if !ok || e.expired(now) {
		return -2
	}
	if e.expiresAt.IsZero() {
		return -1
	}
	return e.expiresAt.Sub(now)
}

// Keys returns all live keys matching a glob pattern (*, ?, [abc]).
func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	out := make([]string, 0)
	for k, e := range s.data {
		if e.expired(now) {
			continue
		}
		if ok, _ := path.Match(pattern, k); ok {
			out = append(out, k)
		}
	}
	return out
}

// Len returns the number of live keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	n := 0
	for _, e := range s.data {
		if !e.expired(now) {
			n++
		}
	}
	return n
}

// Flush removes every key.
func (s *Store) Flush() {
	s.mu.Lock()
	s.data = make(map[string]entry)
	s.mu.Unlock()
}

// StartJanitor launches a background goroutine that removes expired keys
// every interval, so keys nobody reads again don't sit in memory forever.
// Call StopJanitor to shut it down.
func (s *Store) StartJanitor(interval time.Duration) {
	s.stop = make(chan struct{})
	s.done = make(chan struct{})

	go func() {
		defer close(s.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.evictExpired()
			case <-s.stop:
				return
			}
		}
	}()
}

func (s *Store) StopJanitor() {
	if s.stop == nil {
		return
	}
	close(s.stop)
	<-s.done
	s.stop = nil
}

func (s *Store) evictExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	n := 0
	for k, e := range s.data {
		if e.expired(now) {
			delete(s.data, k)
			n++
		}
	}
	return n
}
