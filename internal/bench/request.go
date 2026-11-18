package bench

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"time"

	"model-serving-lab/internal/sse"
	"model-serving-lab/internal/stream"
)

var errDrain = errors.New("benchmark drain deadline")

// execute owns the response body and records a terminal outcome on every path.
// No prompts, server messages or credentials are copied into error records.
func execute(parent context.Context, client *http.Client, c Config, p Prompt, r Record, start time.Time) (out Record) {
	dispatch := time.Now()
	r.DispatchOffsetMS = ptr(ms(dispatch.Sub(start)))
	r.SchedulingDelayMS = ptr(*r.DispatchOffsetMS - r.ScheduledOffsetMS)
	r.GapsMS = []float64{}
	r.Outcome = "failure"
	r.ErrorCode = "transport_error"
	r.Admission = "unknown"
	var limits struct {
		Max        *int `json:"max_tokens"`
		Completion *int `json:"max_completion_tokens"`
	}
	_ = json.Unmarshal([]byte(p.Body), &limits)
	r.RequestedOutputTokens = limits.Max
	if limits.Completion != nil {
		r.RequestedOutputTokens = limits.Completion
	}
	requestCtx, requestCancel := context.WithCancel(parent)
	defer requestCancel()
	ctx, watch := stream.Start(requestCtx, c.Deadlines)
	defer watch.Stop()
	elapsed := func() float64 { return ms(time.Since(dispatch)) }
	var traceMu sync.Mutex
	var firstByte *float64
	var gets, reused, idle int
	defer func() {
		terminal := r.FinishedMS != nil
		if !terminal {
			r.FinishedMS = ptr(elapsed())
		}
		if !terminal {
			if reason, _ := stream.Reason(ctx); reason != "" {
				r.ErrorCode = reason
				if errors.Is(context.Cause(ctx), errDrain) {
					r.ErrorCode = "drain_timeout"
				}
				if reason == "canceled" && !errors.Is(context.Cause(ctx), errDrain) {
					r.ErrorCode = "client_canceled"
				}
				r.Outcome = "failure"
			}
		}
		traceMu.Lock()
		r.FirstByteMS = firstByte
		r.ConnectionGets = gets
		r.ConnectionsReused = reused
		r.ConnectionsIdle = idle
		traceMu.Unlock()
		out = r
	}()
	trace := &httptrace.ClientTrace{
		GotFirstResponseByte: func() {
			traceMu.Lock()
			if firstByte == nil {
				firstByte = ptr(elapsed())
			}
			traceMu.Unlock()
		},
		GotConn: func(i httptrace.GotConnInfo) {
			traceMu.Lock()
			gets++
			if i.Reused {
				reused++
			}
			if i.WasIdle {
				idle++
			}
			traceMu.Unlock()
		},
	}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodPost, c.URL, strings.NewReader(p.Body))
	if err != nil {
		r.ErrorCode = "invalid_url"
		return
	}
	// Disable body rewind as well as application retries. Otherwise Transport
	// can replay a POST after an unwritten failure on a reused connection.
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-Request-ID", r.RequestID)
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	r.HTTPStatus = resp.StatusCode
	r.UpstreamStatus, _ = strconv.Atoi(resp.Header.Get("X-Lab-Upstream-Status"))
	if a := resp.Header.Get("X-Lab-Admitted"); a == "true" || a == "false" {
		r.Admission = a
	}
	r.GatewayRequestID = resp.Header.Get("X-Lab-Request-ID")
	r.Synthetic = r.Synthetic || resp.Header.Get("X-Lab-Synthetic") == "true"

	// Cleanup is independent of terminal outcome: status/error evidence must not
	// become a first-text timeout merely because a response never sends EOF.
	drainResponse := func() []byte {
		timer := time.AfterFunc(5*time.Millisecond, requestCancel)
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		timer.Stop()
		return b
	}
	watch.StartIdle()
	reader := watch.Reader(resp.Body)
	if resp.StatusCode != http.StatusOK {
		r.ErrorCode = "http_" + strconv.Itoa(resp.StatusCode)
		if resp.StatusCode == 429 {
			r.Outcome = "rejected"
			r.ErrorCode = "upstream_429"
			if r.Admission == "false" && r.UpstreamStatus == 0 {
				r.ErrorCode = "gateway_rejected"
			}
		}
		r.FinishedMS = ptr(elapsed())
		// Only structured type/code fields are kept, never the message.
		info := stream.EnvelopeInfo(drainResponse())
		r.ServerErrorType, r.ServerErrorCode = info.Type, info.Code
		return
	}
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "text/event-stream" {
		r.ErrorCode = "bad_content_type"
		return
	}
	decoder := sse.NewDecoder(reader, c.MaxEventBytes)
	for {
		event, err := decoder.Next()
		if err != nil {
			r.ErrorCode = "stream_error"
			if errors.Is(err, io.EOF) {
				r.ErrorCode = "truncated_stream"
			}
			return
		}
		chunk, err := stream.Inspect(event)
		if err != nil {
			r.ErrorCode = "stream_error"
			return
		}
		if chunk.Error != "" {
			r.ErrorCode = "sse_error"
			r.ServerErrorType, r.ServerErrorCode = chunk.ErrorInfo.Type, chunk.ErrorInfo.Code
			r.FinishedMS = ptr(elapsed())
			drainResponse()
			return
		}
		if chunk.Usage != nil {
			r.PromptTokens = ptr(chunk.Usage.PromptTokens)
			r.CompletionTokens = ptr(chunk.Usage.CompletionTokens)
		}
		if chunk.FinishReason != "" {
			r.FinishReason = chunk.FinishReason
		}
		if len(chunk.Text) > 0 {
			now := elapsed()
			if r.FirstTextMS == nil {
				r.FirstTextMS = ptr(now)
				r.ScheduledFirstTextMS = ptr(*r.SchedulingDelayMS + now)
				watch.Text()
			} else {
				r.GapsMS = append(r.GapsMS, now-*r.LastTextMS)
			}
			r.LastTextMS = ptr(now)
			for _, text := range chunk.Text {
				r.TextBytes += len(text)
			}
		}
		if chunk.Done {
			if ctx.Err() != nil {
				return
			}
			r.DoneMS = ptr(elapsed())
			r.Outcome = "success"
			r.ErrorCode = ""
			r.FinishedMS = ptr(elapsed())
			// A short, bounded EOF drain retains healthy HTTP/1.1 connections.
			// Timing ends at the terminal event, not at connection cleanup.
			drainResponse()
			return
		}
	}
}
