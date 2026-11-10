package sse

import (
	"errors"
	"io"
	"strings"
	"testing"
)

type fragmentReader struct {
	s string
	n int
}

func (r *fragmentReader) Read(p []byte) (int, error) {
	if r.s == "" {
		return 0, io.EOF
	}
	n := min(len(p), r.n, len(r.s))
	copy(p, r.s[:n])
	r.s = r.s[n:]
	return n, nil
}
func TestFragmentation(t *testing.T) {
	raw := ": comment\r\nevent: message\r\ndata: first\r\ndata: second\r\n\r\ndata: [DONE]\r\n\r\n"
	for size := 1; size <= len(raw); size++ {
		d := NewDecoder(&fragmentReader{s: raw, n: size}, 1024)
		e, err := d.Next()
		if err != nil || e.Type != "message" || e.Data != "first\nsecond" {
			t.Fatalf("size %d: %+v %v", size, e, err)
		}
		e, err = d.Next()
		if err != nil || e.Data != "[DONE]" {
			t.Fatal(e, err)
		}
		if _, err = d.Next(); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	}
}
func TestBoundsAndEOF(t *testing.T) {
	for _, raw := range []string{"data: " + strings.Repeat("x", 128) + "\n\n", strings.Repeat("data: xx\n", 20) + "\n", ":" + strings.Repeat("x", 128) + "\n\n"} {
		if _, err := NewDecoder(strings.NewReader(raw), 64).Next(); err == nil {
			t.Fatal("accepted oversized event")
		}
	}
	for _, raw := range []string{"", ": comment\n\n", "data: incomplete\n"} {
		if _, err := NewDecoder(strings.NewReader(raw), 64).Next(); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	}
}
func TestEncodeRoundTrip(t *testing.T) {
	for _, e := range []Event{{Data: "[DONE]"}, {Type: "error", Data: "a\n b\n"}, {Data: " leading space"}, {Data: ""}} {
		got, err := NewDecoder(strings.NewReader(string(e.Encode())), 1024).Next()
		if err != nil || got != e {
			t.Fatalf("%q -> %q %v", e, got, err)
		}
	}
}
