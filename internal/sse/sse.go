// Package sse implements bounded, incremental Server-Sent Events decoding.
package sse

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type Event struct {
	Type string
	Data string
}
type Decoder struct {
	scanner *bufio.Scanner
	max     int
}

func NewDecoder(r io.Reader, max int) *Decoder {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, min(4096, max)), max+1)
	return &Decoder{scanner: s, max: max}
}
func (d *Decoder) Next() (Event, error) {
	var e Event
	var data []string
	size := 0
	for d.scanner.Scan() {
		line := d.scanner.Text()
		size += len(line) + 1
		if size > d.max {
			return e, fmt.Errorf("SSE event exceeds %d bytes", d.max)
		}
		if line == "" {
			if len(data) > 0 {
				e.Data = strings.Join(data, "\n")
				return e, nil
			}
			e = Event{}
			size = 0
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "data":
			data = append(data, value)
		case "event":
			e.Type = value
		}
	}
	if err := d.scanner.Err(); err != nil {
		return e, fmt.Errorf("SSE line: %w", err)
	}
	// An unterminated event is never dispatched.
	return Event{}, io.EOF
}

// Encode serialises e with LF framing. Comments, ids and retry fields are not
// preserved; Decode(Encode(e)) yields e.
func (e Event) Encode() []byte {
	var b strings.Builder
	if e.Type != "" {
		b.WriteString("event: " + e.Type + "\n")
	}
	for _, line := range strings.Split(e.Data, "\n") {
		b.WriteString("data: " + line + "\n")
	}
	b.WriteString("\n")
	return []byte(b.String())
}
