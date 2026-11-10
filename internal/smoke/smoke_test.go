package smoke

import (
	"bytes"
	"context"
	"errors"
	"io"
	"model-serving-lab/internal/mockllm"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, e := os.ReadFile("../../testdata/streams/" + name + ".sse")
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestReplay(t *testing.T) {
	for _, fragment := range []int{0, 1, 7, 256} {
		server := httptest.NewServer((mockllm.Server{Fixture: fixture(t, "synthetic-success"), FragmentBytes: fragment}).Handler())
		client := NewClient()
		o := Defaults()
		o.URL = server.URL + "/v1/chat/completions"
		var text bytes.Buffer
		obs, err := Run(context.Background(), client, o, &text)
		if err != nil || obs.Outcome != "success" || !obs.Synthetic || text.String() != "Hello from the synthetic server." {
			t.Fatalf("%+v %s %v", obs, text.String(), err)
		}
		if obs.FirstByte == nil || obs.FirstText == nil || obs.LastText == nil || obs.Done == nil || obs.Finished < *obs.Done || *obs.FirstText > *obs.LastText || *obs.LastText > *obs.Done || obs.PromptTokens == nil || *obs.PromptTokens != 12 || *obs.CompletionTokens != 6 || len(obs.Gaps) != 1 || obs.FinishReason != "stop" {
			t.Fatalf("missing measurement: %+v", obs)
		}
		client.CloseIdleConnections()
		server.Close()
	}
}
func TestFailures(t *testing.T) {
	for _, tc := range []struct {
		name, data, code string
		status           int
	}{
		{"truncated", string(fixture(t, "synthetic-truncated")), "truncated_stream", 200},
		{"error", string(fixture(t, "synthetic-error")), "sse_error", 200},
		{"json-error", "data: {\"error\":{\"message\":\"bad\"}}\n\n", "sse_error", 200},
		// vLLM emits an error data event followed by [DONE] after streaming starts.
		{"error-after-text", "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"error\":{\"message\":\"bad\"}}\n\ndata: [DONE]\n\n", "sse_error", 200},
		{"legacy-error", "data: {\"object\":\"error\",\"message\":\"bad\"}\n\ndata: [DONE]\n\n", "sse_error", 200},
		{"invalid-json", "data: nope\n\n", "stream_error", 200},
		{"oversized", "data: " + strings.Repeat("x", 2048) + "\n\n", "stream_error", 200},
		{"http", "synthetic unavailable", "http_error", 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.data)
			}))
			defer server.Close()
			o := Defaults()
			o.URL = server.URL
			o.MaxEvent = 1024
			client := NewClient()
			defer client.CloseIdleConnections()
			obs, err := Run(context.Background(), client, o, io.Discard)
			if err == nil || obs.Outcome != "failure" || obs.ErrorCode != tc.code || obs.Finished <= 0 {
				t.Fatalf("%+v %v", obs, err)
			}
			if (tc.name == "truncated" || tc.name == "error-after-text") && obs.FirstText == nil {
				t.Fatal("lost text timing")
			}
		})
	}
}
func TestCancellationAndDeadlines(t *testing.T) {
	for _, mode := range []string{"parent", "total", "headers", "first-text", "idle"} {
		t.Run(mode, func(t *testing.T) {
			canceled := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "headers" {
					// Consume the body so the server watches for client disconnect.
					io.Copy(io.Discard, r.Body)
					<-r.Context().Done()
					close(canceled)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				if mode != "first-text" {
					io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
				}
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(canceled)
			}))
			defer server.Close()
			o := Defaults()
			o.URL = server.URL
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "parent":
				time.AfterFunc(30*time.Millisecond, cancel)
			case "total":
				o.Total = 30 * time.Millisecond
			case "headers", "first-text":
				o.FirstText = 30 * time.Millisecond
			case "idle":
				o.Idle = 30 * time.Millisecond
			}
			client := NewClient()
			defer client.CloseIdleConnections()
			obs, err := Run(ctx, client, o, io.Discard)
			expectedCode := "timeout"
			if mode == "parent" {
				expectedCode = "canceled"
			}
			if err == nil || obs.Outcome != "failure" || obs.Done != nil || obs.ErrorCode != expectedCode {
				t.Fatalf("%+v %v", obs, err)
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("upstream not canceled")
			}
		})
	}
}
func TestEarlyStreaming(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"early\"}}]}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o := Defaults()
	o.URL = server.URL
	client := NewClient()
	defer client.CloseIdleConnections()
	received := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { _, err := Run(ctx, client, o, signalWriter{received}); done <- err }()
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("text buffered until completion")
	}
	cancel()
	<-done
}

type signalWriter struct{ ch chan struct{} }

func (w signalWriter) Write(p []byte) (int, error) {
	select {
	case w.ch <- struct{}{}:
	default:
	}
	return len(p), nil
}
func TestOutputError(t *testing.T) {
	server := httptest.NewServer((mockllm.Server{Fixture: fixture(t, "synthetic-success")}).Handler())
	defer server.Close()
	o := Defaults()
	o.URL = server.URL + "/v1/chat/completions"
	client := NewClient()
	defer client.CloseIdleConnections()
	_, err := Run(context.Background(), client, o, badWriter{})
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
}

type badWriter struct{}

func (badWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
