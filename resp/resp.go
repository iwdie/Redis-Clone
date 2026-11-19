// Package resp implements a reader and writer for the Redis Serialization
// Protocol (RESP2).
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// RESP type prefixes.
const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

// Value is a single RESP value.
type Value struct {
	Type  byte
	Str   string
	Num   int64
	Array []Value
	Null  bool
}

// Constructors for common replies.
func Simple(s string) Value              { return Value{Type: SimpleString, Str: s} }
func Err(s string) Value                 { return Value{Type: Error, Str: s} }
func Int(n int64) Value                  { return Value{Type: Integer, Num: n} }
func Bulk(s string) Value                { return Value{Type: BulkString, Str: s} }
func NullBulk() Value                    { return Value{Type: BulkString, Null: true} }
func Arr(vals []Value) Value             { return Value{Type: Array, Array: vals} }
func OK() Value                          { return Simple("OK") }
func Errf(format string, a ...any) Value { return Err(fmt.Sprintf(format, a...)) }

var ErrProtocol = errors.New("protocol error")

// Reader parses RESP values from a stream.
type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

func (r *Reader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// Read parses the next value. Plain inline commands (e.g. "PING\r\n" typed
// into telnet) are returned as an array of bulk strings.
func (r *Reader) Read() (Value, error) {
	prefix, err := r.r.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch prefix {
	case Array:
		return r.readArray()
	case BulkString:
		return r.readBulk()
	case SimpleString, Error:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Type: prefix, Str: line}, nil
	case Integer:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Value{}, ErrProtocol
		}
		return Int(n), nil
	default:
		if err := r.r.UnreadByte(); err != nil {
			return Value{}, err
		}
		return r.readInline()
	}
}

func (r *Reader) readInt() (int, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(line)
	if err != nil {
		return 0, ErrProtocol
	}
	return n, nil
}

func (r *Reader) readArray() (Value, error) {
	n, err := r.readInt()
	if err != nil {
		return Value{}, err
	}
	if n < 0 {
		return Value{Type: Array, Null: true}, nil
	}
	vals := make([]Value, 0, n)
	for i := 0; i < n; i++ {
		v, err := r.Read()
		if err != nil {
			return Value{}, err
		}
		vals = append(vals, v)
	}
	return Arr(vals), nil
}

func (r *Reader) readBulk() (Value, error) {
	n, err := r.readInt()
	if err != nil {
		return Value{}, err
	}
	if n < 0 {
		return NullBulk(), nil
	}
	buf := make([]byte, n+2) // payload + CRLF
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return Value{}, err
	}
	return Bulk(string(buf[:n])), nil
}

func (r *Reader) readInline() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	fields := strings.Fields(line)
	vals := make([]Value, len(fields))
	for i, f := range fields {
		vals[i] = Bulk(f)
	}
	return Arr(vals), nil
}

// Marshal encodes a value in RESP format.
func (v Value) Marshal() []byte {
	var b strings.Builder
	v.write(&b)
	return []byte(b.String())
}

func (v Value) write(b *strings.Builder) {
	switch v.Type {
	case SimpleString, Error:
		b.WriteByte(v.Type)
		b.WriteString(v.Str)
		b.WriteString("\r\n")
	case Integer:
		b.WriteByte(Integer)
		b.WriteString(strconv.FormatInt(v.Num, 10))
		b.WriteString("\r\n")
	case BulkString:
		if v.Null {
			b.WriteString("$-1\r\n")
			return
		}
		b.WriteByte(BulkString)
		b.WriteString(strconv.Itoa(len(v.Str)))
		b.WriteString("\r\n")
		b.WriteString(v.Str)
		b.WriteString("\r\n")
	case Array:
		if v.Null {
			b.WriteString("*-1\r\n")
			return
		}
		b.WriteByte(Array)
		b.WriteString(strconv.Itoa(len(v.Array)))
		b.WriteString("\r\n")
		for _, item := range v.Array {
			item.write(b)
		}
	}
}

// Writer writes RESP values to a stream.
type Writer struct {
	w *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

func (w *Writer) Write(v Value) error {
	if _, err := w.w.Write(v.Marshal()); err != nil {
		return err
	}
	return w.w.Flush()
}
