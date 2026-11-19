package server

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/iwdie/Redis-Clone/resp"
	"github.com/iwdie/Redis-Clone/store"
)

type client struct {
	t    *testing.T
	conn net.Conn
	r    *resp.Reader
	w    *resp.Writer
}

func startServer(t *testing.T) (*Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New("", store.New())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.Serve(ln)
	}()
	t.Cleanup(func() {
		srv.Shutdown()
		<-done
	})
	return srv, ln.Addr().String()
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &client{t: t, conn: c, r: resp.NewReader(c), w: resp.NewWriter(c)}
}

func (c *client) do(args ...string) resp.Value {
	c.t.Helper()
	vals := make([]resp.Value, len(args))
	for i, a := range args {
		vals[i] = resp.Bulk(a)
	}
	if err := c.w.Write(resp.Arr(vals)); err != nil {
		c.t.Fatal(err)
	}
	v, err := c.r.Read()
	if err != nil {
		c.t.Fatal(err)
	}
	return v
}

func TestBasicCommands(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)

	if v := c.do("PING"); v.Str != "PONG" {
		t.Fatalf("PING -> %+v", v)
	}
	if v := c.do("SET", "name", "naveen"); v.Str != "OK" {
		t.Fatalf("SET -> %+v", v)
	}
	if v := c.do("GET", "name"); v.Str != "naveen" {
		t.Fatalf("GET -> %+v", v)
	}
	if v := c.do("SET", "name", "x", "NX"); !v.Null {
		t.Fatalf("SET NX on existing key should return nil, got %+v", v)
	}
	if v := c.do("DEL", "name"); v.Num != 1 {
		t.Fatalf("DEL -> %+v", v)
	}
	if v := c.do("GET", "name"); !v.Null {
		t.Fatalf("GET after DEL -> %+v", v)
	}
}

func TestErrors(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)

	if v := c.do("NOPE"); v.Type != resp.Error {
		t.Fatalf("unknown command -> %+v", v)
	}
	if v := c.do("GET"); v.Type != resp.Error {
		t.Fatalf("wrong arity -> %+v", v)
	}
	c.do("SET", "s", "abc")
	if v := c.do("INCR", "s"); v.Type != resp.Error {
		t.Fatalf("INCR on string -> %+v", v)
	}
}

func TestExpiryOverNetwork(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)

	c.do("SET", "tmp", "1", "PX", "50")
	if v := c.do("PTTL", "tmp"); v.Num <= 0 {
		t.Fatalf("PTTL -> %+v", v)
	}
	time.Sleep(80 * time.Millisecond)
	if v := c.do("GET", "tmp"); !v.Null {
		t.Fatalf("key should have expired, got %+v", v)
	}
	if v := c.do("TTL", "tmp"); v.Num != -2 {
		t.Fatalf("TTL on missing key -> %+v", v)
	}
}

// Many clients incrementing the same key in parallel must not lose updates.
func TestConcurrentClients(t *testing.T) {
	_, addr := startServer(t)

	const clients, perClient = 20, 100
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		c := dial(t, addr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perClient; j++ {
				c.do("INCR", "hits")
			}
		}()
	}
	wg.Wait()

	c := dial(t, addr)
	want := strconv.Itoa(clients * perClient)
	if v := c.do("GET", "hits"); v.Str != want {
		t.Fatalf("hits = %q, want %s", v.Str, want)
	}
}

func TestShutdownClosesClients(t *testing.T) {
	srv, addr := startServer(t)
	c := dial(t, addr)
	c.do("PING")

	srv.Shutdown()

	c.conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c.r.Read(); err == nil {
		t.Fatal("expected connection to be closed after shutdown")
	}
}
