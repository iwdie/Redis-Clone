package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/iwdie/Redis-Clone/resp"
	"github.com/iwdie/Redis-Clone/store"
)

// arity follows Redis: a positive value is an exact argument count
// (including the command name), a negative value is a minimum.
type arity int

func (a arity) ok(n int) bool {
	if a >= 0 {
		return n == int(a)
	}
	return n >= -int(a)
}

type command struct {
	arity arity
	fn    func(s *Server, args []resp.Value) resp.Value
}

var commands map[string]command

func init() {
	commands = map[string]command{
		"PING": {-1, cmdPing},
		"ECHO": {2, cmdEcho},
		"SET":  {-3, cmdSet},
		"GET":  {2, cmdGet},
		"DEL":  {-2, cmdDel},

		"EXISTS":    {-2, cmdExists},
		"APPEND":    {3, cmdAppend},
		"INCR":      {2, cmdIncr},
		"DECR":      {2, cmdDecr},
		"INCRBY":    {3, cmdIncrBy},
		"DECRBY":    {3, cmdDecrBy},
		"EXPIRE":    {3, cmdExpire},
		"PEXPIRE":   {3, cmdPExpire},
		"TTL":       {2, cmdTTL},
		"PTTL":      {2, cmdPTTL},
		"PERSIST":   {2, cmdPersist},
		"PEXPIREAT": {3, cmdPExpireAt},
		"KEYS":      {2, cmdKeys},
		"DBSIZE":    {1, cmdDBSize},
		"FLUSHALL":  {-1, cmdFlushAll},

		// Clients like redis-cli send COMMAND on connect.
		"COMMAND": {-1, func(*Server, []resp.Value) resp.Value { return resp.Arr(nil) }},
	}
}

func strs(args []resp.Value) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = a.Str
	}
	return out
}

func cmdPing(_ *Server, args []resp.Value) resp.Value {
	switch len(args) {
	case 0:
		return resp.Simple("PONG")
	case 1:
		return resp.Bulk(args[0].Str)
	default:
		return resp.Err("ERR wrong number of arguments for 'ping' command")
	}
}

func cmdEcho(_ *Server, args []resp.Value) resp.Value {
	return resp.Bulk(args[0].Str)
}

// SET key value [NX|XX] [EX seconds|PX milliseconds]
func cmdSet(s *Server, args []resp.Value) resp.Value {
	key, val := args[0].Str, args[1].Str
	var opt store.SetOptions

	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i].Str) {
		case "NX":
			opt.NX = true
		case "XX":
			opt.XX = true
		case "EX", "PX":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			n, err := strconv.ParseInt(args[i+1].Str, 10, 64)
			if err != nil || n <= 0 {
				return resp.Err("ERR invalid expire time in 'set' command")
			}
			unit := time.Second
			if strings.EqualFold(args[i].Str, "PX") {
				unit = time.Millisecond
			}
			opt.TTL = time.Duration(n) * unit
			i++
		default:
			return resp.Err("ERR syntax error")
		}
	}
	if opt.NX && opt.XX {
		return resp.Err("ERR syntax error")
	}

	if !s.store.Set(key, val, opt) {
		return resp.NullBulk()
	}
	s.persist("SET", key, val)
	if opt.TTL > 0 {
		// Store an absolute deadline so a restart doesn't extend the TTL.
		s.persist("PEXPIREAT", key, unixMillis(time.Now().Add(opt.TTL)))
	}
	return resp.OK()
}

func cmdGet(s *Server, args []resp.Value) resp.Value {
	v, ok := s.store.Get(args[0].Str)
	if !ok {
		return resp.NullBulk()
	}
	return resp.Bulk(v)
}

func cmdDel(s *Server, args []resp.Value) resp.Value {
	keys := strs(args)
	n := s.store.Del(keys...)
	if n > 0 {
		s.persist(append([]string{"DEL"}, keys...)...)
	}
	return resp.Int(int64(n))
}

func unixMillis(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}

func cmdExists(s *Server, args []resp.Value) resp.Value {
	return resp.Int(int64(s.store.Exists(strs(args)...)))
}

func cmdAppend(s *Server, args []resp.Value) resp.Value {
	n := s.store.Append(args[0].Str, args[1].Str)
	s.persist("APPEND", args[0].Str, args[1].Str)
	return resp.Int(int64(n))
}

func incrBy(s *Server, key string, delta int64) resp.Value {
	n, err := s.store.IncrBy(key, delta)
	if err != nil {
		return resp.Err(err.Error())
	}
	s.persist("INCRBY", key, strconv.FormatInt(delta, 10))
	return resp.Int(n)
}

func parseInt(v resp.Value) (int64, bool) {
	n, err := strconv.ParseInt(v.Str, 10, 64)
	return n, err == nil
}

func cmdIncr(s *Server, args []resp.Value) resp.Value {
	return incrBy(s, args[0].Str, 1)
}

func cmdDecr(s *Server, args []resp.Value) resp.Value {
	return incrBy(s, args[0].Str, -1)
}

func cmdIncrBy(s *Server, args []resp.Value) resp.Value {
	n, ok := parseInt(args[1])
	if !ok {
		return resp.Err(store.ErrNotInteger.Error())
	}
	return incrBy(s, args[0].Str, n)
}

func cmdDecrBy(s *Server, args []resp.Value) resp.Value {
	n, ok := parseInt(args[1])
	if !ok || n == -1<<63 {
		return resp.Err(store.ErrNotInteger.Error())
	}
	return incrBy(s, args[0].Str, -n)
}

func expire(s *Server, args []resp.Value, unit time.Duration) resp.Value {
	n, ok := parseInt(args[1])
	if !ok {
		return resp.Err(store.ErrNotInteger.Error())
	}
	at := time.Now().Add(time.Duration(n) * unit)
	if s.store.ExpireAt(args[0].Str, at) {
		s.persist("PEXPIREAT", args[0].Str, unixMillis(at))
		return resp.Int(1)
	}
	return resp.Int(0)
}

func cmdPExpireAt(s *Server, args []resp.Value) resp.Value {
	ms, ok := parseInt(args[1])
	if !ok {
		return resp.Err(store.ErrNotInteger.Error())
	}
	if s.store.ExpireAt(args[0].Str, time.UnixMilli(ms)) {
		s.persist("PEXPIREAT", args[0].Str, args[1].Str)
		return resp.Int(1)
	}
	return resp.Int(0)
}

func cmdExpire(s *Server, args []resp.Value) resp.Value  { return expire(s, args, time.Second) }
func cmdPExpire(s *Server, args []resp.Value) resp.Value { return expire(s, args, time.Millisecond) }

func ttl(s *Server, key string, unit time.Duration) resp.Value {
	d := s.store.TTL(key)
	if d < 0 {
		return resp.Int(int64(d)) // -1 or -2
	}
	// Round up so a key with 0.4s left reports 1, not 0.
	return resp.Int(int64((d + unit - 1) / unit))
}

func cmdTTL(s *Server, args []resp.Value) resp.Value  { return ttl(s, args[0].Str, time.Second) }
func cmdPTTL(s *Server, args []resp.Value) resp.Value { return ttl(s, args[0].Str, time.Millisecond) }

func cmdPersist(s *Server, args []resp.Value) resp.Value {
	if s.store.Persist(args[0].Str) {
		s.persist("PERSIST", args[0].Str)
		return resp.Int(1)
	}
	return resp.Int(0)
}

func cmdKeys(s *Server, args []resp.Value) resp.Value {
	keys := s.store.Keys(args[0].Str)
	out := make([]resp.Value, len(keys))
	for i, k := range keys {
		out[i] = resp.Bulk(k)
	}
	return resp.Arr(out)
}

func cmdDBSize(s *Server, _ []resp.Value) resp.Value {
	return resp.Int(int64(s.store.Len()))
}

func cmdFlushAll(s *Server, _ []resp.Value) resp.Value {
	s.store.Flush()
	s.persist("FLUSHALL")
	return resp.OK()
}
