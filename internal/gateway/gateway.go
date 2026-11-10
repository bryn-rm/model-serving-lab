// Package gateway is an authenticating, admission-controlled streaming proxy
// for vLLM's OpenAI-compatible chat completions endpoint.
package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"model-serving-lab/internal/sse"
	"model-serving-lab/internal/stream"
)

type Config struct {
	Upstream          string // base URL, e.g. http://127.0.0.1:8000
	HealthPath        string // upstream readiness path; vLLM serves /health
	Model             string
	GatewayKey        string // required from clients
	UpstreamKey       string // sent upstream; optional
	AdmissionCap      int    // 0 disables gateway admission
	RetryAfter        time.Duration
	MaxBodyBytes      int64
	MaxEventBytes     int
	MaxOutputTokens   int
	Deadlines         stream.Deadlines
	WriteTimeout      time.Duration // per downstream write/flush
	BodyReadTimeout   time.Duration
	UpstreamIdleConns int
}

func DefaultConfig() Config {
	return Config{
		Upstream: "http://127.0.0.1:8000", HealthPath: "/health", Model: "lab-model",
		AdmissionCap: 64, RetryAfter: time.Second, MaxBodyBytes: 1 << 20, MaxEventBytes: 1 << 20,
		MaxOutputTokens: 4096, Deadlines: stream.DefaultDeadlines(), WriteTimeout: 10 * time.Second,
		BodyReadTimeout: 10 * time.Second, UpstreamIdleConns: 512,
	}
}

var errShutdown = errors.New("gateway shutting down")

type Gateway struct {
	cfg        Config
	chatURL    string
	healthURL  string
	client     *http.Client
	slots      chan struct{} // nil when admission is disabled
	metrics    *metrics
	log        *slog.Logger
	draining   atomic.Bool
	base       context.Context // canceled to abort remaining streams
	cancelBase context.CancelFunc
	active     sync.WaitGroup
}

func New(cfg Config, log *slog.Logger) (*Gateway, error) {
	u, err := url.Parse(cfg.Upstream)
	switch {
	case err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
		return nil, fmt.Errorf("invalid upstream URL %q", cfg.Upstream)
	case u.User != nil || u.RawQuery != "" || u.Fragment != "":
		return nil, errors.New("upstream URL must not contain credentials, query or fragment")
	case !strings.HasPrefix(cfg.HealthPath, "/") || strings.HasPrefix(cfg.HealthPath, "//"):
		return nil, errors.New("upstream health path must start with a single slash")
	case cfg.GatewayKey == "":
		return nil, errors.New("gateway API key is required")
	case cfg.Model == "":
		return nil, errors.New("model is required")
	case cfg.AdmissionCap < 0:
		return nil, errors.New("admission cap must be >= 0")
	case cfg.RetryAfter < time.Second:
		return nil, errors.New("retry-after must be at least 1s")
	case cfg.MaxBodyBytes <= 0 || cfg.MaxEventBytes <= 0 || cfg.MaxOutputTokens <= 0:
		return nil, errors.New("body, event and output limits must be positive")
	case cfg.WriteTimeout <= 0 || cfg.BodyReadTimeout <= 0 || cfg.UpstreamIdleConns <= 0:
		return nil, errors.New("write/body timeouts and idle connections must be positive")
	}
	if err := cfg.Deadlines.Validate(); err != nil {
		return nil, err
	}
	base := strings.TrimRight(cfg.Upstream, "/")
	if _, err := http.NewRequest(http.MethodGet, base+cfg.HealthPath, nil); err != nil {
		return nil, errors.New("invalid upstream health path")
	}
	g := &Gateway{
		cfg: cfg, chatURL: base + "/v1/chat/completions", healthURL: base + cfg.HealthPath,
		// One reused transport. No per-host connection cap: admission is the
		// only concurrency limit, so the transport never queues silently.
		client: &http.Client{
			Transport:     stream.NewTransport(cfg.UpstreamIdleConns, 0),
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		metrics: newMetrics(cfg.AdmissionCap),
		log:     log,
	}
	if cfg.AdmissionCap > 0 {
		g.slots = make(chan struct{}, cfg.AdmissionCap)
	}
	g.base, g.cancelBase = context.WithCancel(context.Background())
	return g, nil
}

// Handler serves the public API.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", g.chat)
	return mux
}

// AdminHandler serves health, readiness and metrics on the private listener.
func (g *Gateway) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /readyz", g.ready)
	mux.Handle("GET /metrics", promhttp.HandlerFor(g.metrics.registry, promhttp.HandlerOpts{}))
	return mux
}

func (g *Gateway) ready(w http.ResponseWriter, r *http.Request) {
	if g.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, g.healthURL, nil)
	resp, err := g.client.Do(req)
	if err != nil {
		http.Error(w, "upstream unreachable", http.StatusServiceUnavailable)
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		http.Error(w, fmt.Sprintf("upstream health %d", resp.StatusCode), http.StatusServiceUnavailable)
		return
	}
	io.WriteString(w, "ready\n")
}

// record is the per-request log/metric state. It never holds credentials
// or prompt text; the body is identified by length and SHA-256.
type record struct {
	id, clientID   string
	start          time.Time
	admitted       bool
	outcome        string
	status         int
	upstreamStatus int
	upstreamHeader time.Duration
	firstText      time.Duration
	bodyBytes      int
	bodySHA256     string
	errMsg         string
}

var clientIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

func (g *Gateway) chat(w http.ResponseWriter, r *http.Request) {
	g.active.Add(1)
	defer g.active.Done()
	rec := &record{id: rand.Text(), start: time.Now(), outcome: outcomeInvalid}
	defer g.finish(rec)
	h := w.Header()
	h.Set("X-Lab-Request-ID", rec.id)
	h.Set("X-Lab-Admitted", "false")
	rc := http.NewResponseController(w)

	if !g.authorized(r) {
		rec.outcome = outcomeUnauthorized
		h.Set("WWW-Authenticate", `Bearer realm="model-serving-lab"`)
		g.jsonError(w, rc, rec, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "", "missing or invalid gateway API key")
		return
	}
	if id := r.Header.Get("X-Request-ID"); id != "" {
		if !clientIDPattern.MatchString(id) {
			g.jsonError(w, rc, rec, http.StatusBadRequest, "invalid_request_error", "invalid_request_id", "X-Request-ID", "X-Request-ID must match [A-Za-z0-9._:-]{1,128}")
			return
		}
		rec.clientID = id
		h.Set("X-Request-ID", id)
	}
	if media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || media != "application/json" {
		g.jsonError(w, rc, rec, http.StatusUnsupportedMediaType, "invalid_request_error", "unsupported_media_type", "", "Content-Type must be application/json")
		return
	}
	rc.SetReadDeadline(time.Now().Add(g.cfg.BodyReadTimeout))
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, g.cfg.MaxBodyBytes))
	rc.SetReadDeadline(time.Time{})
	if err != nil {
		if errors.As(err, new(*http.MaxBytesError)) {
			g.jsonError(w, rc, rec, http.StatusRequestEntityTooLarge, "invalid_request_error", "body_too_large", "", fmt.Sprintf("request body exceeds %d bytes", g.cfg.MaxBodyBytes))
		} else {
			g.jsonError(w, rc, rec, http.StatusBadRequest, "invalid_request_error", "body_read_error", "", "could not read request body")
		}
		return
	}
	sum := sha256.Sum256(body)
	rec.bodyBytes, rec.bodySHA256 = len(body), hex.EncodeToString(sum[:])
	if verr := ValidateRequest(body, g.cfg.Model, g.cfg.MaxOutputTokens); verr != nil {
		g.jsonError(w, rc, rec, http.StatusBadRequest, "invalid_request_error", verr.Code, verr.Param, verr.Message)
		return
	}

	if !g.acquire() {
		rec.outcome = outcomeRejected
		h.Set("Retry-After", strconv.Itoa(int(g.cfg.RetryAfter/time.Second)))
		g.jsonError(w, rc, rec, http.StatusTooManyRequests, "overloaded_error", "admission_rejected", "", fmt.Sprintf("gateway admission cap %d reached", g.cfg.AdmissionCap))
		return
	}
	defer g.release()
	rec.admitted = true
	h.Set("X-Lab-Admitted", "true")
	g.log.Info("admitted", "request_id", rec.id, "client_request_id", rec.clientID, "body_bytes", rec.bodyBytes, "body_sha256", rec.bodySHA256)
	g.forward(w, rc, r, body, rec)
}

func (g *Gateway) authorized(r *http.Request) bool {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	// Hash both sides so the comparison is constant time regardless of length.
	got, want := sha256.Sum256([]byte(strings.TrimSpace(token))), sha256.Sum256([]byte(g.cfg.GatewayKey))
	return subtle.ConstantTimeCompare(got[:], want[:]) == 1
}

// acquire takes an admission slot without waiting.
func (g *Gateway) acquire() bool {
	if g.slots != nil {
		select {
		case g.slots <- struct{}{}:
		default:
			return false
		}
	}
	g.metrics.inflight.Inc()
	return true
}

func (g *Gateway) release() {
	g.metrics.inflight.Dec()
	if g.slots != nil {
		<-g.slots
	}
}

// forward dispatches the validated body and relays the stream. The caller
// holds an admission slot until this returns.
func (g *Gateway) forward(w http.ResponseWriter, rc *http.ResponseController, r *http.Request, body []byte, rec *record) {
	reqCtx, cancel := context.WithCancelCause(r.Context())
	defer cancel(nil)
	stopShutdown := context.AfterFunc(g.base, func() { cancel(errShutdown) })
	defer stopShutdown()
	ctx, watch := stream.Start(reqCtx, g.cfg.Deadlines)
	defer watch.Stop()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.chatURL, bytes.NewReader(body))
	if err != nil {
		rec.outcome, rec.errMsg = outcomeUpstreamDown, err.Error()
		g.jsonError(w, rc, rec, http.StatusBadGateway, "upstream_error", "upstream_unavailable", "", "could not build upstream request")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-Request-ID", rec.id)
	if g.cfg.UpstreamKey != "" {
		req.Header.Set("Authorization", "Bearer "+g.cfg.UpstreamKey)
	}
	req = req.WithContext(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(i httptrace.GotConnInfo) { g.metrics.upstream.WithLabelValues(strconv.FormatBool(i.Reused)).Inc() },
	}))
	// No retries: the client performs none for a POST that may have reached
	// the upstream, and the gateway never re-sends.
	resp, err := g.client.Do(req)
	if err != nil {
		rec.errMsg = err.Error()
		switch reason := g.reason(ctx); reason {
		case outcomeClientCanceled:
			rec.outcome = reason // nobody to answer
		case outcomeShutdown:
			rec.outcome = reason
			g.jsonError(w, rc, rec, http.StatusServiceUnavailable, "upstream_error", reason, "", "gateway shutting down")
		case "":
			rec.outcome = outcomeUpstreamDown
			g.jsonError(w, rc, rec, http.StatusBadGateway, "upstream_error", outcomeUpstreamDown, "", "upstream connection failed")
		default:
			rec.outcome = reason
			g.jsonError(w, rc, rec, http.StatusGatewayTimeout, "timeout_error", reason, "", "upstream did not respond before the "+reason)
		}
		return
	}
	defer resp.Body.Close()
	rec.upstreamStatus, rec.upstreamHeader = resp.StatusCode, time.Since(rec.start)

	if resp.StatusCode != http.StatusOK {
		g.relayUpstreamError(w, rc, resp, rec)
		return
	}
	if media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type")); err != nil || media != "text/event-stream" {
		rec.outcome = outcomeUpstreamHTTP
		g.jsonError(w, rc, rec, http.StatusBadGateway, "upstream_error", "upstream_bad_content_type", "", "upstream did not return an event stream")
		return
	}

	h := w.Header()
	if v := resp.Header.Get("X-Lab-Synthetic"); v != "" {
		h.Set("X-Lab-Synthetic", v)
	}
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	rec.status = http.StatusOK
	if err := g.writeDeadline(rc, func() error { w.WriteHeader(http.StatusOK); return rc.Flush() }); err != nil {
		rec.outcome, rec.errMsg = outcomeClientWriteError, err.Error()
		return
	}

	watch.StartIdle()
	decoder := sse.NewDecoder(watch.Reader(resp.Body), g.cfg.MaxEventBytes)
	upstreamFailed := false
	for {
		event, err := decoder.Next()
		if err != nil {
			reason := g.reason(ctx)
			switch {
			case reason == outcomeClientCanceled:
				rec.outcome = reason
			case reason != "":
				rec.outcome, rec.errMsg = reason, err.Error()
				g.sseError(w, rc, reason, "stream ended: "+strings.ReplaceAll(reason, "_", " "))
			case upstreamFailed:
				rec.outcome = outcomeUpstreamError
			case errors.Is(err, io.EOF):
				rec.outcome = outcomeTruncated
				g.sseError(w, rc, "upstream_truncated", "upstream stream ended before its done event")
			default:
				rec.outcome, rec.errMsg = outcomeStreamError, err.Error()
				g.sseError(w, rc, "upstream_stream_error", "invalid upstream stream")
			}
			return
		}
		chunk, err := stream.Inspect(event)
		if err != nil {
			rec.outcome, rec.errMsg = outcomeStreamError, err.Error()
			g.sseError(w, rc, "upstream_stream_error", "invalid upstream event")
			return
		}
		if len(chunk.Text) > 0 && rec.firstText == 0 {
			rec.firstText = time.Since(rec.start)
			watch.Text()
		}
		if chunk.Error != "" {
			upstreamFailed = true // forwarded below; terminal status stays failure
		}
		encoded := event.Encode()
		if err := g.writeDeadline(rc, func() error {
			if _, err := w.Write(encoded); err != nil {
				return err
			}
			return rc.Flush()
		}); err != nil {
			// A deadline error means a stalled reader. Otherwise the client
			// went away (net/http cancels r.Context on any write failure, so
			// the error type, not the context, separates the two).
			rec.outcome, rec.errMsg = outcomeClientWriteError, err.Error()
			if !errors.Is(err, os.ErrDeadlineExceeded) && r.Context().Err() != nil {
				rec.outcome = outcomeClientCanceled
			}
			return
		}
		if chunk.Done {
			rec.outcome = outcomeSuccess
			if upstreamFailed {
				rec.outcome = outcomeUpstreamError
			}
			// [DONE] is terminal. Closing the body releases capacity immediately,
			// even if an upstream leaves its HTTP response open. The transport
			// can reuse connections whose bodies have already reached EOF.
			return
		}
	}
}

// reason classifies why ctx ended: "" (live or not a context failure),
// client_canceled, shutdown, or a deadline name.
func (g *Gateway) reason(ctx context.Context) string {
	reason, cause := stream.Reason(ctx)
	switch {
	case reason == "":
		return ""
	case errors.Is(cause, errShutdown):
		return outcomeShutdown
	case reason == "canceled":
		return outcomeClientCanceled
	}
	return reason
}

// relayUpstreamError passes client-caused 4xx responses through (bounded) and
// reports upstream auth failures and other statuses as 502.
func (g *Gateway) relayUpstreamError(w http.ResponseWriter, rc *http.ResponseController, resp *http.Response, rec *record) {
	rec.outcome = outcomeUpstreamHTTP
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	code := resp.StatusCode
	w.Header().Set("X-Lab-Upstream-Status", strconv.Itoa(code))
	if code >= 400 && code < 500 && code != http.StatusUnauthorized && code != http.StatusForbidden && json.Valid(b) {
		if v := resp.Header.Get("Retry-After"); v != "" {
			w.Header().Set("Retry-After", v)
		}
		rec.status = code
		g.writeBody(w, rc, code, b)
		return
	}
	g.jsonError(w, rc, rec, http.StatusBadGateway, "upstream_error", "upstream_status", "", fmt.Sprintf("upstream returned HTTP %d", code))
}

func (g *Gateway) jsonError(w http.ResponseWriter, rc *http.ResponseController, rec *record, status int, typ, code, param, msg string) {
	rec.status = status
	if rec.errMsg == "" {
		rec.errMsg = code
	}
	body, _ := json.Marshal(map[string]any{"error": map[string]any{"message": msg, "type": typ, "code": code, "param": param, "request_id": rec.id}})
	g.writeBody(w, rc, status, append(body, '\n'))
}

func (g *Gateway) writeBody(w http.ResponseWriter, rc *http.ResponseController, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	g.writeDeadline(rc, func() error { w.WriteHeader(status); _, err := w.Write(body); return err })
}

// sseError terminates an already-started stream with an error event. No
// [DONE] follows, so clients also see the stream as incomplete.
func (g *Gateway) sseError(w http.ResponseWriter, rc *http.ResponseController, code, msg string) {
	data, _ := json.Marshal(map[string]any{"error": map[string]any{"message": msg, "type": "gateway_error", "code": code}})
	ev := sse.Event{Data: string(data)}.Encode()
	g.writeDeadline(rc, func() error {
		if _, err := w.Write(ev); err != nil {
			return err
		}
		return rc.Flush()
	})
}

// writeDeadline sets a fresh downstream write deadline before each write so
// a stalled client cannot hold an admission slot indefinitely.
func (g *Gateway) writeDeadline(rc *http.ResponseController, write func() error) error {
	if err := rc.SetWriteDeadline(time.Now().Add(g.cfg.WriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return write()
}

func (g *Gateway) finish(rec *record) {
	elapsed := time.Since(rec.start)
	admitted := strconv.FormatBool(rec.admitted)
	g.metrics.requests.WithLabelValues(rec.outcome, admitted).Inc()
	g.metrics.duration.WithLabelValues(rec.outcome).Observe(elapsed.Seconds())
	if rec.admitted && rec.firstText > 0 {
		g.metrics.firstText.Observe(rec.firstText.Seconds())
	}
	attrs := []any{
		"request_id", rec.id, "client_request_id", rec.clientID, "admitted", rec.admitted,
		"outcome", rec.outcome, "status", rec.status, "upstream_status", rec.upstreamStatus,
		"duration_ms", ms(elapsed), "upstream_header_ms", ms(rec.upstreamHeader),
		"first_text_ms", ms(rec.firstText), "body_bytes", rec.bodyBytes, "body_sha256", rec.bodySHA256,
	}
	level := slog.LevelInfo
	if rec.outcome != outcomeSuccess {
		level = slog.LevelWarn
		if rec.errMsg == "" {
			rec.errMsg = rec.outcome
		}
		attrs = append(attrs, "error", rec.errMsg)
	}
	g.log.Log(context.Background(), level, "request finished", attrs...)
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// Serve runs the public and admin listeners until ctx ends, then drains:
// readiness fails, new connections stop, and in-flight streams get grace to
// finish. After grace, remaining streams receive an SSE shutdown error and
// connections close. Total shutdown is bounded by grace + WriteTimeout + 1s.
func (g *Gateway) Serve(ctx context.Context, public, admin net.Listener, grace time.Duration) error {
	errLog := slog.NewLogLogger(g.log.Handler(), slog.LevelWarn)
	pub := &http.Server{Handler: g.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, ErrorLog: errLog}
	adm := &http.Server{Handler: g.AdminHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, ErrorLog: errLog}
	errc := make(chan error, 2)
	go func() { errc <- pub.Serve(public) }()
	go func() { errc <- adm.Serve(admin) }()
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errc:
	}
	g.log.Info("draining", "grace", grace.String())
	g.draining.Store(true)
	drain, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	if err := pub.Shutdown(drain); err != nil {
		g.log.Warn("grace expired; canceling remaining streams")
		g.cancelBase()
		done := make(chan struct{})
		go func() { g.active.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(g.cfg.WriteTimeout + time.Second):
		}
		pub.Close()
	}
	g.cancelBase()
	admCtx, admCancel := context.WithTimeout(context.Background(), time.Second)
	defer admCancel()
	if adm.Shutdown(admCtx) != nil {
		adm.Close()
	}
	g.client.CloseIdleConnections()
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}
