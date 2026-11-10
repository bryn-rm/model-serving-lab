// Package mockllm serves synthetic protocol fixtures, never model inference.
package mockllm

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"
)

type Server struct {
	Fixture       []byte
	Delay         time.Duration
	FragmentBytes int
}

func (s Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Lab-Synthetic", "true")
		controller := http.NewResponseController(w)
		events := bytes.SplitAfter(s.Fixture, []byte("\n\n"))
		for _, event := range events {
			if len(event) == 0 {
				continue
			}
			if !pause(r.Context(), s.Delay) {
				return
			}
			step := s.FragmentBytes
			if step <= 0 {
				step = len(event)
			}
			for len(event) > 0 {
				n := min(step, len(event))
				if controller.SetWriteDeadline(time.Now().Add(10*time.Second)) != nil {
					return
				}
				if _, err := w.Write(event[:n]); err != nil {
					return
				}
				if controller.Flush() != nil {
					return
				}
				event = event[n:]
			}
		}
	})
	return mux
}
func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
