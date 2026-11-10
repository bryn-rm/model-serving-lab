package smoke

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"model-serving-lab/internal/sse"
	"model-serving-lab/internal/stream"
)

type Options struct {
	URL, Model, Prompt, APIKey string
	Total, FirstText, Idle     time.Duration
	MaxEvent                   int
}

func Defaults() Options {
	return Options{URL: "http://127.0.0.1:8000/v1/chat/completions", Model: "lab-model", Prompt: "Say hello.", Total: 300 * time.Second, FirstText: 120 * time.Second, Idle: 120 * time.Second, MaxEvent: 1 << 20}
}

type Observation struct {
	Synthetic        bool      `json:"synthetic"`
	SchemaVersion    int       `json:"schema_version"`
	StartedAt        string    `json:"started_at"`
	Target           string    `json:"target"`
	Model            string    `json:"model"`
	FirstByte        *float64  `json:"first_byte_ms"`
	FirstText        *float64  `json:"first_text_ms"`
	LastText         *float64  `json:"last_text_ms"`
	Done             *float64  `json:"done_ms"`
	Finished         float64   `json:"finished_ms"`
	Gaps             []float64 `json:"text_chunk_gaps_ms"`
	PromptTokens     *int      `json:"prompt_tokens"`
	CompletionTokens *int      `json:"completion_tokens"`
	HTTPStatus       int       `json:"http_status"`
	Outcome          string    `json:"outcome"`
	ErrorCode        string    `json:"error_code,omitempty"`
	Error            string    `json:"error,omitempty"`
	Admission        string    `json:"admission"`
	FinishReason     string    `json:"finish_reason,omitempty"`
	ConnectionReused bool      `json:"connection_reused"`
}

func NewClient() *http.Client {
	return &http.Client{Transport: stream.NewTransport(512, 256), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func Run(parent context.Context, client *http.Client, opt Options, output io.Writer) (obs Observation, result error) {
	start := time.Now()
	obs = Observation{SchemaVersion: 1, StartedAt: start.UTC().Format(time.RFC3339Nano), Target: opt.URL, Model: opt.Model, Outcome: "failure", Admission: "unknown", Gaps: []float64{}}
	elapsed := func() float64 { return float64(time.Since(start)) / float64(time.Millisecond) }
	var mu sync.Mutex
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		obs.Finished = elapsed()
		if result != nil {
			obs.Error = result.Error()
			if obs.ErrorCode == "" {
				obs.ErrorCode = "stream_error"
			}
		}
	}()
	deadlines := stream.Deadlines{Total: opt.Total, FirstText: opt.FirstText, Idle: opt.Idle}
	if deadlines.Validate() != nil || opt.MaxEvent <= 0 {
		return obs, errors.New("deadlines and event limit must be positive")
	}
	ctx, watch := stream.Start(parent, deadlines)
	defer watch.Stop()
	defer func() {
		if result == nil {
			return
		}
		if reason, cause := stream.Reason(ctx); reason != "" {
			obs.ErrorCode = "canceled"
			if strings.HasSuffix(reason, "_timeout") {
				obs.ErrorCode = "timeout"
			}
			result = fmt.Errorf("%w: %v", cause, result)
		}
	}()
	body, _ := json.Marshal(map[string]any{"model": opt.Model, "messages": []map[string]string{{"role": "user", "content": opt.Prompt}}, "stream": true, "max_tokens": 128, "stream_options": map[string]bool{"include_usage": true}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, opt.URL, bytes.NewReader(body))
	if err != nil {
		return obs, err
	}
	req.Header.Set("Content-Type", "application/json")
	if opt.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+opt.APIKey)
	}
	trace := &httptrace.ClientTrace{GotFirstResponseByte: func() { mu.Lock(); defer mu.Unlock(); v := elapsed(); obs.FirstByte = &v }, GotConn: func(i httptrace.GotConnInfo) { mu.Lock(); defer mu.Unlock(); obs.ConnectionReused = i.Reused }}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := client.Do(req)
	if err != nil {
		obs.ErrorCode = "transport_error"
		return obs, err
	}
	defer resp.Body.Close()
	obs.HTTPStatus = resp.StatusCode
	obs.Synthetic = resp.Header.Get("X-Lab-Synthetic") == "true"
	if a := resp.Header.Get("X-Lab-Admitted"); a == "true" || a == "false" {
		obs.Admission = a
	}
	if resp.StatusCode != http.StatusOK {
		obs.ErrorCode = "http_error"
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return obs, fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
	}
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "text/event-stream" {
		return obs, errors.New("expected text/event-stream")
	}
	watch.StartIdle()
	decoder := sse.NewDecoder(watch.Reader(resp.Body), opt.MaxEvent)
	for {
		event, err := decoder.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				obs.ErrorCode = "truncated_stream"
				return obs, errors.New("stream ended without [DONE]")
			}
			return obs, err
		}
		chunk, err := stream.Inspect(event)
		if err != nil {
			return obs, err
		}
		if chunk.Error != "" {
			obs.ErrorCode = "sse_error"
			return obs, fmt.Errorf("upstream error: %s", chunk.Error)
		}
		if chunk.Done {
			v := elapsed()
			obs.Done = &v
			obs.Outcome = "success"
			return obs, nil
		}
		if chunk.Usage != nil {
			obs.PromptTokens = &chunk.Usage.PromptTokens
			obs.CompletionTokens = &chunk.Usage.CompletionTokens
		}
		if chunk.FinishReason != "" {
			obs.FinishReason = chunk.FinishReason
		}
		for _, text := range chunk.Text {
			v := elapsed()
			if obs.FirstText == nil {
				obs.FirstText = &v
				watch.Text()
			} else {
				obs.Gaps = append(obs.Gaps, v-*obs.LastText)
			}
			obs.LastText = &v
			if _, err := io.WriteString(output, text); err != nil {
				return obs, err
			}
		}
	}
}
