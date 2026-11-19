# Redis Clone in Go

A Redis-style in-memory key-value store written in Go. It speaks the real
Redis wire protocol (RESP), so you can talk to it with `redis-cli`, any
Redis client library, or plain `nc`/`telnet`.

## Features

- **RESP protocol** parser/serializer, plus inline commands for telnet
- **Concurrent clients**: one goroutine per connection; the accept loop hands
  connections to the server over a channel
- **Mutex-based synchronization**: the keyspace is guarded by a
  `sync.RWMutex`, so reads run in parallel and writes are exclusive
- **Key expiry**: lazy expiry on read + a background janitor goroutine
- **AOF persistence**: write commands go through a buffered channel to a
  single writer goroutine and are fsynced every second; replayed on startup
- **Graceful shutdown** on Ctrl+C / SIGTERM via a quit channel and `sync.WaitGroup`

## Supported commands

| Group     | Commands |
|-----------|----------|
| Strings   | `SET key value [NX\|XX] [EX s\|PX ms]`, `GET`, `APPEND` |
| Counters  | `INCR`, `DECR`, `INCRBY`, `DECRBY` |
| Keys      | `DEL`, `EXISTS`, `KEYS pattern`, `DBSIZE`, `FLUSHALL` |
| Expiry    | `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST` |
| Misc      | `PING`, `ECHO`, `QUIT`, `COMMAND` |

## Getting started

```bash
git clone https://github.com/iwdie/Redis-Clone.git
cd Redis-Clone
go run . -addr :6379 -aof appendonly.aof
```

Flags:

| Flag    | Default          | Description |
|---------|------------------|-------------|
| `-addr` | `:6379`          | Address to listen on |
| `-aof`  | `appendonly.aof` | AOF path; pass `-aof ""` to disable persistence |

Then, from another terminal:

```bash
$ redis-cli
127.0.0.1:6379> SET name naveen EX 60
OK
127.0.0.1:6379> GET name
"naveen"
127.0.0.1:6379> INCR visits
(integer) 1
127.0.0.1:6379> TTL name
(integer) 58
```

No `redis-cli`? Use netcat:

```bash
printf 'PING\r\nSET a 1\r\nGET a\r\n' | nc localhost 6379
```

## Project structure

```
.
├── main.go            # flags, AOF setup, signal handling
├── resp/              # RESP reader/writer
├── store/             # thread-safe keyspace (RWMutex), TTLs, janitor
├── server/            # TCP server, connection handling, command table
└── aof/               # append-only file writer + replay
```

## How concurrency works

```
            ┌──────────────┐  conns chan  ┌────────────┐
 clients ──▶│ accept loop  │─────────────▶│ main loop  │── go handle(conn) ──┐
            └──────────────┘              └────────────┘                      │
                                                 ▲ quit chan                 ▼
                                                 │                 ┌──────────────────┐
                                           Shutdown()              │ store (RWMutex)  │
                                                                   └──────────────────┘
                                                                           │ entries chan
                                                                           ▼
                                                                   ┌──────────────────┐
                                                                   │ AOF writer       │
                                                                   └──────────────────┘
```

## Tests

```bash
go test -race ./...
```

The suite includes tests where many goroutines/clients increment the same key
at once, which fail (or trip the race detector) if synchronization is wrong.

## Benchmark

With the server running, you can use the official tool:

```bash
redis-benchmark -p 6379 -t set,get,incr -n 100000 -c 50
```
