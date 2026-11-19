package resp

import (
	"bytes"
	"strings"
	"testing"
)

func TestReadArrayOfBulk(t *testing.T) {
	r := NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"))
	v, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != Array || len(v.Array) != 2 {
		t.Fatalf("unexpected value: %+v", v)
	}
	if v.Array[0].Str != "GET" || v.Array[1].Str != "key" {
		t.Fatalf("unexpected args: %+v", v.Array)
	}
}

func TestReadInline(t *testing.T) {
	r := NewReader(strings.NewReader("SET  name  naveen\r\n"))
	v, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Array) != 3 || v.Array[2].Str != "naveen" {
		t.Fatalf("unexpected inline parse: %+v", v)
	}
}

func TestBulkWithCRLFInside(t *testing.T) {
	r := NewReader(strings.NewReader("$7\r\na\r\nb\r\nc\r\n"))
	v, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	if v.Str != "a\r\nb\r\nc" {
		t.Fatalf("got %q", v.Str)
	}
}

func TestMarshal(t *testing.T) {
	cases := []struct {
		v    Value
		want string
	}{
		{OK(), "+OK\r\n"},
		{Err("ERR boom"), "-ERR boom\r\n"},
		{Int(42), ":42\r\n"},
		{Bulk("hi"), "$2\r\nhi\r\n"},
		{NullBulk(), "$-1\r\n"},
		{Arr([]Value{Bulk("a"), Int(1)}), "*2\r\n$1\r\na\r\n:1\r\n"},
	}
	for _, c := range cases {
		if got := string(c.v.Marshal()); got != c.want {
			t.Errorf("Marshal(%+v) = %q, want %q", c.v, got, c.want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	in := Arr([]Value{Bulk("SET"), Bulk("k"), Bulk(""), Int(-7), NullBulk()})
	var buf bytes.Buffer
	if err := NewWriter(&buf).Write(in); err != nil {
		t.Fatal(err)
	}
	out, err := NewReader(&buf).Read()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(in.Marshal(), out.Marshal()) {
		t.Fatalf("round trip mismatch: %q vs %q", in.Marshal(), out.Marshal())
	}
}
