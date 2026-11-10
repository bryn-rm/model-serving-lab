// Package stream holds the chat-completion streaming rules shared by the
// smoke client and the gateway: deadlines, event inspection and transport.
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"model-serving-lab/internal/sse"
)

// Deadline causes. Each wraps context.DeadlineExceeded.
var (
	ErrFirstText = fmt.Errorf("first text deadline: %w", context.DeadlineExceeded)
	ErrIdle      = fmt.Errorf("stream idle deadline: %w", context.DeadlineExceeded)
	ErrTotal     = fmt.Errorf("total deadline: %w", context.DeadlineExceeded)
)

// Deadlines are the shared client/gateway deadline policy. FirstText also
// covers the wait for response headers; Idle starts once headers arrive.
type Deadlines struct{ Total, FirstText, Idle time.Duration }

func DefaultDeadlines() Deadlines {
	return Deadlines{Total: 300 * time.Second, FirstText: 120 * time.Second, Idle: 120 * time.Second}
}

func (d Deadlines) Validate() error {
	if d.Total <= 0 || d.FirstText <= 0 || d.Idle <= 0 {
		return errors.New("deadlines must be positive")
	}
	return nil
}

// Watch enforces Deadlines on a context. Methods other than Stop must be
// called from the goroutine that reads the stream.
type Watch struct {
	cancel      context.CancelCauseFunc
	totalCancel context.CancelFunc
	first, idle *time.Timer
	idleFor     time.Duration
}

// Start derives a context that ends at the total or first-text deadline.
func Start(parent context.Context, d Deadlines) (context.Context, *Watch) {
	total, totalCancel := context.WithTimeoutCause(parent, d.Total, ErrTotal)
	ctx, cancel := context.WithCancelCause(total)
	w := &Watch{cancel: cancel, totalCancel: totalCancel, idleFor: d.Idle}
	w.first = time.AfterFunc(d.FirstText, func() { cancel(ErrFirstText) })
	return ctx, w
}

// StartIdle arms the idle deadline; call once response headers arrive.
func (w *Watch) StartIdle() {
	w.idle = time.AfterFunc(w.idleFor, func() { w.cancel(ErrIdle) })
}

// Text records that nonempty text arrived, disarming the first-text deadline.
func (w *Watch) Text() { w.first.Stop() }

// Reader resets the idle deadline whenever bytes arrive from r.
func (w *Watch) Reader(r io.Reader) io.Reader { return &activityReader{r: r, w: w} }

func (w *Watch) Stop() {
	w.first.Stop()
	if w.idle != nil {
		w.idle.Stop()
	}
	w.cancel(nil)
	w.totalCancel()
}

type activityReader struct {
	r io.Reader
	w *Watch
}

func (r *activityReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 && r.w.idle != nil {
		r.w.idle.Reset(r.w.idleFor)
	}
	return n, err
}

// Reason explains why ctx ended: "" while live, a deadline name, or the
// non-deadline cause (context.Canceled for caller cancellation).
func Reason(ctx context.Context) (string, error) {
	if ctx.Err() == nil {
		return "", nil
	}
	cause := context.Cause(ctx)
	switch {
	case errors.Is(cause, ErrFirstText):
		return "first_text_timeout", cause
	case errors.Is(cause, ErrIdle):
		return "idle_timeout", cause
	case errors.Is(cause, ErrTotal), errors.Is(cause, context.DeadlineExceeded):
		return "total_timeout", cause
	}
	return "canceled", cause
}

type Usage struct{ PromptTokens, CompletionTokens int }

// Chunk is what the lab needs from one SSE event.
type Chunk struct {
	Done         bool
	Error        string   // nonempty when the event reports an upstream error
	Text         []string // nonempty content deltas, in choice order
	FinishReason string
	Usage        *Usage
}

// Inspect classifies an event. Errors are `event: error`, a non-null
// top-level "error" (current vLLM) or "object":"error" (older vLLM).
// Invalid JSON is returned as an error.
func Inspect(e sse.Event) (Chunk, error) {
	if e.Type == "error" {
		message := e.Data
		if message == "" {
			message = "upstream error event"
		}
		return Chunk{Error: message}, nil
	}
	if e.Data == "[DONE]" {
		return Chunk{Done: true}, nil
	}
	var raw struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
		Error  json.RawMessage `json:"error"`
		Object string          `json:"object"`
	}
	if err := json.Unmarshal([]byte(e.Data), &raw); err != nil {
		return Chunk{}, fmt.Errorf("invalid stream JSON: %w", err)
	}
	if len(raw.Error) > 0 && string(raw.Error) != "null" {
		return Chunk{Error: string(raw.Error)}, nil
	}
	if raw.Object == "error" {
		return Chunk{Error: e.Data}, nil
	}
	var c Chunk
	if raw.Usage != nil {
		c.Usage = &Usage{PromptTokens: raw.Usage.PromptTokens, CompletionTokens: raw.Usage.CompletionTokens}
	}
	for _, choice := range raw.Choices {
		if choice.FinishReason != nil {
			c.FinishReason = *choice.FinishReason
		}
		if choice.Delta.Content != "" {
			c.Text = append(c.Text, choice.Delta.Content)
		}
	}
	return c, nil
}

// NewTransport returns the shared HTTP/1.1 transport. maxConns 0 means no
// per-host limit (the gateway relies on admission, not hidden transport
// queueing). No response-header timeout: the first-text deadline covers it.
func NewTransport(idleConns, maxConns int) *http.Transport {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	return &http.Transport{
		Protocols:           protocols,
		ForceAttemptHTTP2:   false,
		MaxIdleConns:        idleConns,
		MaxIdleConnsPerHost: idleConns,
		MaxConnsPerHost:     maxConns,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
}
