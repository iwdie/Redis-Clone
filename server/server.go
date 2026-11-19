// Package server accepts TCP clients speaking RESP and executes their
// commands against a shared store.
package server

import (
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/iwdie/Redis-Clone/aof"
	"github.com/iwdie/Redis-Clone/resp"
	"github.com/iwdie/Redis-Clone/store"
)

// Server is a Redis-compatible TCP server.
//
// Concurrency model:
//   - one goroutine runs the accept loop and hands new connections to the
//     main loop over a channel;
//   - every client gets its own goroutine that reads, executes and replies;
//   - shared data lives in store.Store, which is protected by an RWMutex;
//   - a quit channel broadcasts shutdown to every goroutine.
type Server struct {
	addr  string
	store *store.Store
	aof   *aof.AOF // nil when persistence is disabled

	ln   net.Listener
	quit chan struct{}
	wg   sync.WaitGroup

	mu      sync.Mutex
	clients map[net.Conn]struct{}
}

func New(addr string, st *store.Store) *Server {
	return &Server{
		addr:    addr,
		store:   st,
		quit:    make(chan struct{}),
		clients: make(map[net.Conn]struct{}),
	}
}

// ListenAndServe binds to the configured address and serves until Shutdown.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln until Shutdown is called.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	log.Printf("listening on %s", ln.Addr())

	conns := make(chan net.Conn)
	errs := make(chan error, 1)

	// Accept loop: blocks on Accept and forwards each connection.
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				errs <- err
				return
			}
			select {
			case conns <- c:
			case <-s.quit:
				c.Close()
				return
			}
		}
	}()

	for {
		select {
		case c := <-conns:
			s.track(c)
			s.wg.Add(1)
			go s.handle(c)
		case err := <-errs:
			select {
			case <-s.quit:
				s.wg.Wait() // listener closed by Shutdown
				return nil
			default:
				return err
			}
		case <-s.quit:
			// Wait for in-flight clients so callers can safely close
			// resources (like the AOF) once Serve returns.
			s.wg.Wait()
			return nil
		}
	}
}

// LoadAOF replays an append-only file into the store. Call it before
// SetAOF so replayed commands are not written back to the file.
func (s *Server) LoadAOF(path string) (int, error) {
	return aof.Replay(path, func(args []resp.Value) {
		s.dispatch(args)
	})
}

// SetAOF enables persistence of write commands.
func (s *Server) SetAOF(a *aof.AOF) {
	s.aof = a
}

// persist records a write command in the AOF, if one is configured.
func (s *Server) persist(args ...string) {
	if s.aof != nil {
		s.aof.Append(args...)
	}
}

// Addr returns the listener address (useful when started on port 0).
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

func (s *Server) track(c net.Conn) {
	s.mu.Lock()
	s.clients[c] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.clients, c)
	s.mu.Unlock()
}

func (s *Server) handle(c net.Conn) {
	defer s.wg.Done()
	defer s.untrack(c)
	defer c.Close()

	r := resp.NewReader(c)
	w := resp.NewWriter(c)

	for {
		v, err := r.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("client %s: %v", c.RemoteAddr(), err)
			}
			return
		}
		if v.Type != resp.Array || len(v.Array) == 0 {
			continue
		}

		reply, closeAfter := s.dispatch(v.Array)
		if err := w.Write(reply); err != nil {
			return
		}
		if closeAfter {
			return
		}
	}
}

// dispatch runs a single command and reports whether the connection
// should be closed afterwards (QUIT).
func (s *Server) dispatch(args []resp.Value) (resp.Value, bool) {
	name := strings.ToUpper(args[0].Str)
	if name == "QUIT" {
		return resp.OK(), true
	}

	cmd, ok := commands[name]
	if !ok {
		return resp.Errf("ERR unknown command '%s'", args[0].Str), false
	}
	if !cmd.arity.ok(len(args)) {
		return resp.Errf("ERR wrong number of arguments for '%s' command", strings.ToLower(name)), false
	}
	return cmd.fn(s, args[1:]), false
}

// Shutdown stops accepting clients, closes open connections and waits for
// every client goroutine to finish.
func (s *Server) Shutdown() {
	select {
	case <-s.quit:
		return // already shut down
	default:
		close(s.quit)
	}

	s.mu.Lock()
	if s.ln != nil {
		s.ln.Close()
	}
	for c := range s.clients {
		c.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
}
