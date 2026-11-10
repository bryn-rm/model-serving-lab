package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"model-serving-lab/internal/mockllm"
	"model-serving-lab/internal/smoke"
	"model-serving-lab/internal/stream"
)

const gatewayKey, upstreamKey = "gw-secret", "up-secret"

var validBody = `{"model":"lab-model","messages":[{"role":"user","content":"hi"}],"stream":true,"max_tokens":16}`

func testConfig(upstream string) Config {
	cfg := DefaultConfig()
	cfg.Upstream, cfg.GatewayKey, cfg.UpstreamKey = upstream, gatewayKey, upstreamKey
	cfg.Deadlines = stream.Deadlines{Total: 5 * time.Second, FirstText: 5 * time.Second, Idle: 5 * time.Second}
	cfg.WriteTimeout = 2 * time.Second
	return cfg
}

// start runs a gateway in front of upstream; edit adjusts the config.
func start(t *testing.T, upstream http.Handler, edit func(*Config)) (*Gateway, string) {
	t.Helper()
	up := httptest.NewServer(upstream)
	t.Cleanup(up.Close)
	cfg := testConfig(up.URL)
	if edit != nil {
		edit(&cfg)
	}
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(g.Handler())
	t.Cleanup(func() { gw.Close(); g.client.CloseIdleConnections() })
	return g, gw.URL + "/v1/chat/completions"
}

func post(ctx context.Context, t *testing.T, url, key, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func errorCode(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	var e struct {
		Error struct{ Code string } `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	return e.Error.Code
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func outcomes(g *Gateway, outcome string, admitted bool) float64 {
	return testutil.ToFloat64(g.metrics.requests.WithLabelValues(outcome, fmt.Sprint(admitted)))
}

// idle asserts all capacity was released.
func idle(t *testing.T, g *Gateway) {
	t.Helper()
	waitFor(t, "capacity release", func() bool { return testutil.ToFloat64(g.metrics.inflight) == 0 && len(g.slots) == 0 })
}

func event(data string) string { return "data: " + data + "\n\n" }

const textEvent = `{"choices":[{"delta":{"content":"hi"},"finish_reason":null}]}`

func sseHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()
}

func send(w http.ResponseWriter, s string) {
	io.WriteString(w, s)
	w.(http.Flusher).Flush()
}

func fixture(t *testing.T, name string) []byte {
	b, err := os.ReadFile("../../testdata/streams/" + name + ".sse")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func smokeVia(t *testing.T, url string) (smoke.Observation, string, error) {
	o := smoke.Defaults()
	o.URL, o.APIKey = url, gatewayKey
	client := smoke.NewClient()
	defer client.CloseIdleConnections()
	var text bytes.Buffer
	obs, err := smoke.Run(context.Background(), client, o, &text)
	return obs, text.String(), err
}

func TestValidateRequest(t *testing.T) {
	ok := []string{
		validBody,
		`{"model":"lab-model","messages":[{"role":"system","content":"s"},{"role":"user","content":"u"}],"stream":true,"max_completion_tokens":4096,"temperature":0,"top_p":1,"seed":42,"ignore_eos":true,"n":1,"stream_options":{"include_usage":true}}`,
	}
	for _, body := range ok {
		if err := ValidateRequest([]byte(body), "lab-model", 4096); err != nil {
			t.Errorf("rejected valid body: %v\n%s", err, body)
		}
	}
	bad := map[string]string{
		"not json":           `nope`,
		"trailing data":      validBody + `{}`,
		"array":              `[]`,
		"invalid utf8":       "{\"model\":\"\xff\"}",
		"unsupported field":  strings.Replace(validBody, `"stream":true`, `"stream":true,"tools":[]`, 1),
		"case-variant field": strings.Replace(validBody, `"stream":true`, `"stream":true,"Max_Tokens":1`, 1),
		"null field":         strings.Replace(validBody, `"stream":true`, `"stream":true,"seed":null`, 1),
		"missing model":      strings.Replace(validBody, `"model":"lab-model",`, ``, 1),
		"wrong model":        strings.Replace(validBody, `lab-model`, `other`, 1),
		"stream false":       strings.Replace(validBody, `"stream":true`, `"stream":false`, 1),
		"stream string":      strings.Replace(validBody, `"stream":true`, `"stream":"true"`, 1),
		"n=2":                strings.Replace(validBody, `"stream":true`, `"stream":true,"n":2`, 1),
		"empty messages":     `{"model":"lab-model","messages":[],"stream":true,"max_tokens":16}`,
		"empty content":      strings.Replace(validBody, `"content":"hi"`, `"content":" "`, 1),
		"array content":      strings.Replace(validBody, `"content":"hi"`, `"content":[{"type":"text","text":"hi"}]`, 1),
		"bad role":           strings.Replace(validBody, `"role":"user"`, `"role":"tool"`, 1),
		"message extra":      strings.Replace(validBody, `"content":"hi"`, `"content":"hi","name":"x"`, 1),
		"both limits":        strings.Replace(validBody, `"max_tokens":16`, `"max_tokens":16,"max_completion_tokens":16`, 1),
		"no limit":           strings.Replace(validBody, `,"max_tokens":16`, ``, 1),
		"limit too large":    strings.Replace(validBody, `16`, `4097`, 1),
		"limit zero":         strings.Replace(validBody, `16`, `0`, 1),
		"limit fractional":   strings.Replace(validBody, `16`, `16.5`, 1),
		"temperature high":   strings.Replace(validBody, `"stream":true`, `"stream":true,"temperature":2.5`, 1),
		"top_p zero":         strings.Replace(validBody, `"stream":true`, `"stream":true,"top_p":0`, 1),
		"seed float":         strings.Replace(validBody, `"stream":true`, `"stream":true,"seed":1.5`, 1),
		"ignore_eos string":  strings.Replace(validBody, `"stream":true`, `"stream":true,"ignore_eos":"yes"`, 1),
		"stream_options key": strings.Replace(validBody, `"stream":true`, `"stream":true,"stream_options":{"continuous_usage_stats":true}`, 1),
		"include_usage str":  strings.Replace(validBody, `"stream":true`, `"stream":true,"stream_options":{"include_usage":"yes"}`, 1),
	}
	for name, body := range bad {
		if err := ValidateRequest([]byte(body), "lab-model", 4096); err == nil {
			t.Errorf("%s: accepted %s", name, body)
		}
	}
}

func TestAuthenticationAndCredentialSeparation(t *testing.T) {
	var calls atomic.Int32
	var seen atomic.Value
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		seen.Store(r.Header.Get("Authorization"))
		sseHeaders(w)
		send(w, event("[DONE]"))
	})
	g, url := start(t, upstream, nil)
	for _, key := range []string{"", "wrong", upstreamKey} {
		resp := post(context.Background(), t, url, key, validBody)
		if resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("X-Lab-Admitted") != "false" || resp.Header.Get("WWW-Authenticate") == "" || errorCode(t, resp) != "invalid_api_key" {
			t.Fatalf("key %q: status %d", key, resp.StatusCode)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unauthenticated request reached upstream")
	}
	resp := post(context.Background(), t, url, gatewayKey, validBody)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || seen.Load() != "Bearer "+upstreamKey {
		t.Fatalf("status %d, upstream saw %q", resp.StatusCode, seen.Load())
	}
	waitFor(t, "unauthorized metric", func() bool { return outcomes(g, outcomeUnauthorized, false) == 3 })

	// Without an upstream key no Authorization header is sent upstream.
	_, url = start(t, upstream, func(c *Config) { c.UpstreamKey = "" })
	resp = post(context.Background(), t, url, gatewayKey, validBody)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if seen.Load() != "" {
		t.Fatalf("gateway credential leaked upstream: %q", seen.Load())
	}
}

func TestRequestRejectionsBeforeUpstream(t *testing.T) {
	var calls atomic.Int32
	_, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }), func(c *Config) { c.MaxBodyBytes = 256 })
	cases := []struct {
		body, contentType, requestID string
		status                       int
		code                         string
	}{
		{body: `{"model":"lab-model"}`, status: 400, code: "missing_field"},
		{body: validBody, contentType: "text/plain", status: 415, code: "unsupported_media_type"},
		{body: validBody, requestID: "bad id!", status: 400, code: "invalid_request_id"},
		{body: strings.Replace(validBody, "hi", strings.Repeat("x", 300), 1), status: 413, code: "body_too_large"},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer "+gatewayKey)
		req.Header.Set("Content-Type", "application/json")
		if tc.contentType != "" {
			req.Header.Set("Content-Type", tc.contentType)
		}
		if tc.requestID != "" {
			req.Header.Set("X-Request-ID", tc.requestID)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != tc.status || resp.Header.Get("X-Lab-Admitted") != "false" || resp.Header.Get("X-Lab-Request-ID") == "" {
			t.Fatalf("%s: status %d", tc.code, resp.StatusCode)
		}
		if code := errorCode(t, resp); code != tc.code {
			t.Fatalf("want %s, got %s", tc.code, code)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("rejected request reached upstream")
	}
}

func TestBodyParity(t *testing.T) {
	// Unusual but valid formatting: key order, whitespace, escapes, number
	// spellings and vLLM extensions must reach the upstream byte-for-byte.
	body := "{ \"stream\" : true,\n\t\"messages\":[{\"content\":\"caf\\u00e9 \\\"q\\\" ☃\",\"role\":\"user\"}],\n" +
		"\"model\":\"lab-model\", \"max_tokens\": 128, \"temperature\": 0.0, \"top_p\": 1e0, \"seed\": 7, " +
		"\"ignore_eos\": true, \"n\": 1, \"stream_options\": {\"include_usage\": true} }  \n"
	var got atomic.Value
	var upstreamID atomic.Value
	_, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.Store(b)
		upstreamID.Store(r.Header.Get("X-Request-ID"))
		sseHeaders(w)
		send(w, event("[DONE]"))
	}), nil)
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+gatewayKey)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-Request-ID", "bench-run1:000042")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Equal(got.Load().([]byte), []byte(body)) {
		t.Fatalf("status %d; upstream body differs:\n%q\n%q", resp.StatusCode, got.Load(), body)
	}
	if resp.Header.Get("X-Request-ID") != "bench-run1:000042" || resp.Header.Get("X-Lab-Request-ID") != upstreamID.Load() || resp.Header.Get("X-Lab-Admitted") != "true" {
		t.Fatalf("ids/admission headers: %v (upstream id %v)", resp.Header, upstreamID.Load())
	}
}

func TestStreamRelayAndOutcomes(t *testing.T) {
	cases := []struct {
		name, fixture, smokeCode, outcome, text string
	}{
		{"success", string(fixture(t, "synthetic-success")), "", outcomeSuccess, "Hello from the synthetic server."},
		{"sse-error", string(fixture(t, "synthetic-error")), "sse_error", outcomeUpstreamError, ""},
		{"truncated", string(fixture(t, "synthetic-truncated")), "sse_error", outcomeTruncated, "Hello from the synthetic server."},
		{"error-after-text", event(textEvent) + event(`{"error":{"message":"boom"}}`) + event("[DONE]"), "sse_error", outcomeUpstreamError, "hi"},
		{"legacy-error", event(textEvent) + event(`{"object":"error","message":"boom"}`) + event("[DONE]"), "sse_error", outcomeUpstreamError, "hi"},
		{"invalid-json", event(textEvent) + event("nope"), "sse_error", outcomeStreamError, "hi"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// One-byte flushed fragments exercise reassembly across reads.
			g, url := start(t, mockllm.Server{Fixture: []byte(tc.fixture), FragmentBytes: 1}.Handler(), nil)
			obs, text, err := smokeVia(t, url)
			if text != tc.text || obs.Admission != "true" || !obs.Synthetic {
				t.Fatalf("text %q admission %s synthetic %v", text, obs.Admission, obs.Synthetic)
			}
			if tc.smokeCode == "" {
				if err != nil || obs.Outcome != "success" || obs.PromptTokens == nil || *obs.PromptTokens != 12 || obs.FinishReason != "stop" {
					t.Fatalf("%+v %v", obs, err)
				}
			} else if err == nil || obs.ErrorCode != tc.smokeCode {
				t.Fatalf("want client %s, got %+v %v", tc.smokeCode, obs, err)
			}
			if tc.text != "" && obs.FirstText == nil {
				t.Fatal("first text timing lost")
			}
			waitFor(t, "outcome "+tc.outcome, func() bool { return outcomes(g, tc.outcome, true) == 1 })
			idle(t, g)
		})
	}
}

func TestEarlyStreaming(t *testing.T) {
	release := make(chan struct{})
	_, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sseHeaders(w)
		send(w, event(textEvent))
		select {
		case <-release:
			send(w, event("[DONE]"))
		case <-r.Context().Done():
		}
	}), nil)
	defer close(release)
	resp := post(context.Background(), t, url, gatewayKey, validBody)
	defer resp.Body.Close()
	got := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(resp.Body).ReadString('\n')
		got <- line
	}()
	select {
	case line := <-got:
		if !strings.Contains(line, `"content":"hi"`) {
			t.Fatalf("unexpected first line %q", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gateway buffered the stream until upstream completion")
	}
}

// holdingUpstream streams one text event and holds the request open until
// released or canceled. It reports cancellations.
type holdingUpstream struct {
	calls    atomic.Int32
	release  chan struct{}
	canceled chan struct{}
}

func newHolding() *holdingUpstream {
	return &holdingUpstream{release: make(chan struct{}), canceled: make(chan struct{}, 16)}
}

func (h *holdingUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.calls.Add(1)
	io.Copy(io.Discard, r.Body)
	sseHeaders(w)
	send(w, event(textEvent))
	select {
	case <-h.release:
		send(w, event("[DONE]"))
	case <-r.Context().Done():
		h.canceled <- struct{}{}
	}
}

// openStream starts a request and waits for its first event.
func openStream(ctx context.Context, t *testing.T, url string) *http.Response {
	t.Helper()
	resp := post(ctx, t, url, gatewayKey, validBody)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestOverloadRejection(t *testing.T) {
	up := newHolding()
	g, url := start(t, up, func(c *Config) { c.AdmissionCap = 1; c.RetryAfter = 2 * time.Second })
	first := openStream(context.Background(), t, url)
	began := time.Now()
	resp := post(context.Background(), t, url, gatewayKey, validBody)
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") != "2" || resp.Header.Get("X-Lab-Admitted") != "false" || errorCode(t, resp) != "admission_rejected" {
		t.Fatalf("want 429, got %d %v", resp.StatusCode, resp.Header)
	}
	if time.Since(began) > time.Second {
		t.Fatal("rejection was not immediate")
	}
	if up.calls.Load() != 1 {
		t.Fatal("rejected request reached upstream")
	}
	close(up.release)
	io.Copy(io.Discard, first.Body)
	first.Body.Close()
	idle(t, g)
	resp = post(context.Background(), t, url, gatewayKey, validBody)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Lab-Admitted") != "true" {
		t.Fatalf("capacity not recovered: %d", resp.StatusCode)
	}
	if outcomes(g, outcomeRejected, false) != 1 {
		t.Fatal("429 not counted")
	}
}

func TestAdmissionDisabled(t *testing.T) {
	up := newHolding()
	g, url := start(t, up, func(c *Config) { c.AdmissionCap = 0 })
	var open []*http.Response
	for range 5 {
		open = append(open, openStream(context.Background(), t, url))
	}
	if testutil.ToFloat64(g.metrics.inflight) != 5 || testutil.ToFloat64(g.metrics.capacity) != 0 {
		t.Fatal("cap 0 should admit everything")
	}
	close(up.release)
	for _, r := range open {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	idle(t, g)
}

func TestClientCancellationPropagates(t *testing.T) {
	up := newHolding()
	g, url := start(t, up, func(c *Config) { c.AdmissionCap = 1 })
	ctx, cancel := context.WithCancel(context.Background())
	resp := openStream(ctx, t, url)
	cancel()
	resp.Body.Close()
	select {
	case <-up.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("client cancellation did not reach upstream")
	}
	waitFor(t, "client_canceled outcome", func() bool { return outcomes(g, outcomeClientCanceled, true) == 1 })
	idle(t, g)
}

func TestDeadlines(t *testing.T) {
	short := 150 * time.Millisecond
	cases := []struct {
		name   string
		edit   func(*Config)
		before bool // deadline fires before upstream headers
	}{
		{"first_text_timeout", func(c *Config) { c.Deadlines.FirstText = short }, true},
		{"first_text_timeout", func(c *Config) { c.Deadlines.FirstText = short }, false},
		{"idle_timeout", func(c *Config) { c.Deadlines.Idle = short }, false},
		{"total_timeout", func(c *Config) { c.Deadlines.Total = short }, false},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/before-headers=%v", tc.name, tc.before), func(t *testing.T) {
			canceled := make(chan struct{}, 1)
			g, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if !tc.before {
					sseHeaders(w)
					if tc.name != "first_text_timeout" {
						send(w, event(textEvent))
					}
				}
				<-r.Context().Done()
				canceled <- struct{}{}
			}), func(c *Config) { c.AdmissionCap = 1; tc.edit(c) })
			resp := post(context.Background(), t, url, gatewayKey, validBody)
			if tc.before {
				if resp.StatusCode != http.StatusGatewayTimeout || errorCode(t, resp) != tc.name {
					t.Fatalf("want 504 %s, got %d", tc.name, resp.StatusCode)
				}
			} else {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 || !strings.Contains(string(b), `"code":"`+tc.name+`"`) || strings.Contains(string(b), "[DONE]") {
					t.Fatalf("want SSE error %s, got %q", tc.name, b)
				}
			}
			select {
			case <-canceled:
			case <-time.After(2 * time.Second):
				t.Fatal("upstream not canceled at deadline")
			}
			waitFor(t, tc.name+" outcome", func() bool { return outcomes(g, tc.name, true) == 1 })
			idle(t, g)
		})
	}
}

func TestCapacityRecoveryAfterFailures(t *testing.T) {
	var scenario atomic.Value
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		switch scenario.Load() {
		case "http500":
			http.Error(w, "boom", 500)
		case "hangup":
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
		case "unauthorized":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(401)
			io.WriteString(w, `{"error":"bad key"}`)
		case "bad-content-type":
			io.WriteString(w, "hello")
		case "truncated":
			sseHeaders(w)
			send(w, event(textEvent))
		case "sse-error":
			sseHeaders(w)
			send(w, event(`{"error":{"message":"boom"}}`)+event("[DONE]"))
		case "oversized":
			sseHeaders(w)
			send(w, event(strings.Repeat("x", 2048)))
		case "stall":
			sseHeaders(w)
			<-r.Context().Done()
		default:
			sseHeaders(w)
			send(w, event(textEvent)+event("[DONE]"))
		}
	})
	g, url := start(t, upstream, func(c *Config) {
		c.AdmissionCap = 1
		c.MaxEventBytes = 1024
		c.Deadlines.FirstText = 100 * time.Millisecond
	})
	for _, s := range []string{"http500", "hangup", "unauthorized", "bad-content-type", "truncated", "sse-error", "oversized", "stall", "client-cancel"} {
		scenario.Store(s)
		if s == "client-cancel" {
			scenario.Store("stall")
			ctx, cancel := context.WithCancel(context.Background())
			resp := post(ctx, t, url, gatewayKey, validBody)
			cancel()
			resp.Body.Close()
		} else {
			resp := post(context.Background(), t, url, gatewayKey, validBody)
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 && strings.HasSuffix(string(b), event("[DONE]")) && s != "sse-error" {
				t.Fatalf("%s: reported as success: %q", s, b)
			}
		}
		idle(t, g)
		scenario.Store("ok")
		resp := post(context.Background(), t, url, gatewayKey, validBody)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.HasSuffix(string(b), event("[DONE]")) {
			t.Fatalf("after %s: capacity not recovered (%d %q)", s, resp.StatusCode, b)
		}
	}
	if n := outcomes(g, outcomeSuccess, true); n != 9 {
		t.Fatalf("want 9 successes, got %v", n)
	}
}

func TestUpstreamClientErrorRelayed(t *testing.T) {
	_, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		io.WriteString(w, `{"error":{"message":"prompt too long","code":"context_length"}}`)
	}), nil)
	resp := post(context.Background(), t, url, gatewayKey, validBody)
	if resp.StatusCode != 400 || resp.Header.Get("X-Lab-Upstream-Status") != "400" || errorCode(t, resp) != "context_length" {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestStalledClient(t *testing.T) {
	canceled := make(chan struct{}, 1)
	big := event(`{"choices":[{"delta":{"content":"` + strings.Repeat("x", 32<<10) + `"}}]}`)
	var calls atomic.Int32
	g, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		sseHeaders(w)
		if calls.Add(1) > 1 { // later requests complete normally
			send(w, event(textEvent)+event("[DONE]"))
			return
		}
		for r.Context().Err() == nil {
			send(w, big)
		}
		canceled <- struct{}{}
	}), func(c *Config) { c.AdmissionCap = 1; c.WriteTimeout = 200 * time.Millisecond })

	// A raw client that sends a request and never reads the response.
	host := strings.TrimPrefix(strings.TrimSuffix(url, "/v1/chat/completions"), "http://")
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.(*net.TCPConn).SetReadBuffer(4096)
	fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", host, gatewayKey, len(validBody), validBody)

	select {
	case <-canceled:
	case <-time.After(10 * time.Second):
		t.Fatal("stalled client kept the upstream stream open")
	}
	waitFor(t, "client_write_error outcome", func() bool { return outcomes(g, outcomeClientWriteError, true) == 1 })
	idle(t, g)
	obs, _, err := smokeVia(t, url)
	if err != nil || obs.Admission != "true" {
		t.Fatalf("slot not released after stalled client: %+v %v", obs, err)
	}
}

func TestReadinessAndMetrics(t *testing.T) {
	up := httptest.NewServer(mockllm.Server{Fixture: fixture(t, "synthetic-success")}.Handler())
	defer up.Close()
	cfg := testConfig(up.URL)
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	admin := httptest.NewServer(g.AdminHandler())
	defer admin.Close()
	get := func(path string) (int, string) {
		resp, err := http.Get(admin.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, _ := get("/healthz"); code != 200 {
		t.Fatal("healthz")
	}
	if code, body := get("/readyz"); code != 200 {
		t.Fatalf("readyz with healthy upstream: %d %s", code, body)
	}
	g.metrics.requests.WithLabelValues(outcomeRejected, "false").Inc()
	code, body := get("/metrics")
	for _, want := range []string{`lab_gateway_requests_total{admitted="false",outcome="rejected"} 1`, "lab_gateway_admission_cap 64", "lab_gateway_inflight_requests 0", "go_goroutines"} {
		if code != 200 || !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q", want)
		}
	}
	g.draining.Store(true)
	if code, _ := get("/readyz"); code != 503 {
		t.Fatal("readyz while draining")
	}
	g.draining.Store(false)
	up.Close()
	if code, _ := get("/readyz"); code != 503 {
		t.Fatal("readyz with upstream down")
	}
}

func TestBoundedShutdown(t *testing.T) {
	up := newHolding()
	upstream := httptest.NewServer(up)
	defer upstream.Close()
	cfg := testConfig(upstream.URL)
	cfg.WriteTimeout = 500 * time.Millisecond
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	public, _ := net.Listen("tcp", "127.0.0.1:0")
	admin, _ := net.Listen("tcp", "127.0.0.1:0")
	ctx, stop := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- g.Serve(ctx, public, admin, 200*time.Millisecond) }()

	resp := post(context.Background(), t, "http://"+public.Addr().String()+"/v1/chat/completions", gatewayKey, validBody)
	reader := bufio.NewReader(resp.Body)
	reader.ReadString('\n')
	began := time.Now()
	stop()
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown not bounded")
	}
	if elapsed := time.Since(began); elapsed < 200*time.Millisecond {
		t.Fatalf("in-flight stream got no grace (%v)", elapsed)
	}
	rest, _ := io.ReadAll(reader)
	resp.Body.Close()
	if !strings.Contains(string(rest), `"code":"shutdown"`) {
		t.Fatalf("stream not terminated with shutdown error: %q", rest)
	}
	select {
	case <-up.canceled:
	case <-time.After(time.Second):
		t.Fatal("upstream not canceled on shutdown")
	}
	if outcomes(g, outcomeShutdown, true) != 1 || testutil.ToFloat64(g.metrics.inflight) != 0 {
		t.Fatal("shutdown outcome or slot release missing")
	}
}

func TestDoneReleasesCapacityWithoutEOF(t *testing.T) {
	canceled := make(chan struct{}, 1)
	g, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sseHeaders(w)
		send(w, event(textEvent)+event("[DONE]"))
		<-r.Context().Done()
		canceled <- struct{}{}
	}), func(c *Config) { c.AdmissionCap = 1 })
	resp := post(context.Background(), t, url, gatewayKey, validBody)
	defer resp.Body.Close()
	// Do not close the downstream body to force upstream cancellation.
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("gateway waited for upstream EOF after DONE")
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil || !strings.HasSuffix(string(b), event("[DONE]")) {
		t.Fatalf("terminal response: %q, %v", b, err)
	}
	idle(t, g)
	if outcomes(g, outcomeSuccess, true) != 1 {
		t.Fatal("missing success outcome")
	}
}

func TestDuplicateKeysRejectedBeforeDispatch(t *testing.T) {
	var calls atomic.Int32
	_, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}), nil)
	for _, body := range []string{
		strings.Replace(validBody, `"stream":true`, `"stream":false,"stream":true`, 1),
		strings.Replace(validBody, `"stream":true`, `"stream":false,"str\u0065am":true`, 1),
		strings.Replace(validBody, `"content":"hi"`, `"content":"hidden","content":"hi"`, 1),
		strings.TrimSuffix(validBody, "}") + `,"stream_options":{"include_usage":false,"include_usage":true}}`,
	} {
		resp := post(context.Background(), t, url, gatewayKey, body)
		if resp.StatusCode != 400 || errorCode(t, resp) != "invalid_json" {
			t.Fatalf("duplicate accepted: %d", resp.StatusCode)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid body dispatched upstream")
	}
}

func TestInvalidUpstreamConfiguration(t *testing.T) {
	for _, edit := range []func(*Config){
		func(c *Config) { c.Upstream = "http://user:secret@localhost:8000" },
		func(c *Config) { c.Upstream += "?secret=value" },
		func(c *Config) { c.Upstream += "#fragment" },
		func(c *Config) { c.HealthPath = "health" },
		func(c *Config) { c.HealthPath = "//other-host/health" },
		func(c *Config) { c.HealthPath = "/bad\npath" },
	} {
		cfg := testConfig("http://localhost:8000")
		edit(&cfg)
		if _, err := New(cfg, slog.Default()); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}

func TestEmptyUpstreamErrorIsFailure(t *testing.T) {
	g, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sseHeaders(w)
		send(w, "event: error\ndata:\n\n"+event("[DONE]"))
	}), nil)
	resp := post(context.Background(), t, url, gatewayKey, validBody)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	waitFor(t, "upstream error outcome", func() bool { return outcomes(g, outcomeUpstreamError, true) == 1 })
	idle(t, g)
}
