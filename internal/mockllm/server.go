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
	APIKey        string // when set, chat requests must send this bearer token
}

func (s Server) Handler() http.Handler {
	mux := http.NewServeMux()
	health := func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) }
	mux.HandleFunc("GET /healthz", health)
	mux.HandleFunc("GET /health", health) // vLLM's readiness path
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if s.APIKey != "" && r.Header.Get("Authorization") != "Bearer "+s.APIKey {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":{"message":"synthetic mock: invalid API key","type":"authentication_error"}}` + "\n"))
			return
		}
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
