package bench

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"model-serving-lab/internal/stream"
)

const textEvent = "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n"
const doneEvent = "data: [DONE]\n\n"

func workload() Workload {
	body := `{ "model":"lab-model", "messages":[{"role":"user","content":"hi"}], "stream":true, "max_tokens":128 }`
	return Workload{SchemaVersion: 1, Synthetic: true, Prompts: []Prompt{{ID: "p", Body: body, SHA256: hash([]byte(body))}}}
}
func config(t *testing.T, url string) Config {
	t.Helper()
	c := Defaults()
	c.URL = url
	c.OutputRoot = t.TempDir()
	c.Attempts = 20
	c.Repeats = 1
	c.Warmup = 0
	c.Workers = 3
	c.MaxInflight = 3
	c.MaxDuration = time.Second
	c.Drain = 100 * time.Millisecond
	c.Deadlines = stream.Deadlines{Total: time.Second, FirstText: time.Second, Idle: time.Second}
	return c
}
func success(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	io.WriteString(w, textEvent+"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":6}}\n\n"+doneEvent)
}
func client() *http.Client { return &http.Client{Transport: stream.NewTransport(512, 0)} }

func TestClosedAccountingParityAndReuse(t *testing.T) {
	var in, maxIn, calls atomic.Int32
	body := workload().Prompts[0].Body
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if string(b) != body {
			t.Error("body was rewritten")
		}
		if r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("X-Request-ID") == "" {
			t.Error("missing identity/auth")
		}
		n := in.Add(1)
		for old := maxIn.Load(); n > old; old = maxIn.Load() {
			if maxIn.CompareAndSwap(old, n) {
				break
			}
		}
		defer in.Add(-1)
		calls.Add(1)
		time.Sleep(2 * time.Millisecond)
		success(w)
	}))
	defer up.Close()
	c := config(t, up.URL)
	c.APIKey = "secret"
	c.Repeats = 3
	c.Attempts = 40
	c.Warmup = 2
	dir, e := Run(context.Background(), c, workload(), Schedule{})
	if e != nil {
		t.Fatal(e)
	}
	records, partial, e := ReadRecords(filepath.Join(dir, "requests.jsonl"))
	if e != nil || partial || len(records) != 120 {
		t.Fatalf("records=%d partial=%v error=%v", len(records), partial, e)
	}
	if maxIn.Load() > 3 || maxIn.Load() < 2 || calls.Load() != 126 {
		t.Fatalf("concurrency=%d calls=%d", maxIn.Load(), calls.Load())
	}
	sum, e := Summarize(dir)
	if e != nil {
		t.Fatal(e)
	}
	for _, s := range sum.Repeats {
		if !s.Complete || s.Counts["success"] != 40 || s.Counts["offered"] != 40 || s.Counts["admission_unknown"] != 40 || s.Counts["connections_reused"] < 35 || s.Counts["output_length_deviation"] != 40 {
			t.Fatalf("%+v", s.Counts)
		}
	}
	manifest, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	var metadata Manifest
	if err := json.Unmarshal(manifest, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.BodySHA256["p"] != hash([]byte(body)) || string(metadata.RequestSettings["p"]["max_tokens"]) != "128" || metadata.RequestSettings["p"]["messages"] != nil {
		t.Fatal("manifest omitted workload identity/settings or exposed prompts")
	}
	if strings.Contains(string(manifest), "secret") {
		t.Fatal("credential leaked")
	}
	saved, e := LoadWorkload(filepath.Join(dir, "workload.json"))
	if e != nil || saved.Prompts[0].Body != body {
		t.Fatal("snapshot changed bytes")
	}
	if sum.AcrossCompleteRepeats["counts.success"].Repeats != 3 {
		t.Fatal("missing repeat aggregate")
	}
}
func TestOpenBoundsDropsAndScheduledTiming(t *testing.T) {
	var in, peak atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		n := in.Add(1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		defer in.Add(-1)
		time.Sleep(30 * time.Millisecond)
		success(w)
	}))
	defer up.Close()
	c := config(t, up.URL)
	c.Mode = "open"
	c.MaxInflight = 2
	c.Workers = 1
	s := Schedule{SchemaVersion: 1, WindowMS: 20}
	for i := 0; i < 20; i++ {
		s.Arrivals = append(s.Arrivals, Arrival{OffsetMS: float64(i), PromptID: "p"})
	}
	dir, e := Run(context.Background(), c, workload(), s)
	if e != nil {
		t.Fatal(e)
	}
	sum, e := Summarize(dir)
	if e != nil {
		t.Fatal(e)
	}
	counts := sum.Repeats[0].Counts
	if counts["offered"] != 20 || counts["dispatched"] != 2 || counts["dropped"] != 18 || peak.Load() > 2 {
		t.Fatalf("%+v peak=%d", counts, peak.Load())
	}
	rr, _, _ := ReadRecords(filepath.Join(dir, "requests.jsonl"))
	for _, r := range rr {
		if r.Outcome == "dropped" {
			if r.FinishedMS != nil || r.DispatchOffsetMS != nil || r.Admission != "false" {
				t.Fatal("drop has dispatch timing")
			}
		} else if r.ScheduledFirstTextMS == nil || *r.ScheduledFirstTextMS < *r.FirstTextMS {
			t.Fatal("missing scheduled timing")
		}
	}
	if !sum.Repeats[0].Offered.MetricsMS["penalised_first_text"].P95.Unbounded {
		t.Fatal("drops removed from penalty")
	}
}
func TestRequestFailuresKeepTextAndAdmission(t *testing.T) {
	for _, mode := range []string{"timeout", "idle", "first", "cancel", "sse", "empty-error", "legacy", "truncated", "gateway429", "upstream429", "direct429"} {
		t.Run(mode, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if strings.Contains(mode, "429") {
					if mode == "gateway429" {
						w.Header().Set("X-Lab-Admitted", "false")
					} else if mode == "upstream429" {
						w.Header().Set("X-Lab-Admitted", "true")
						w.Header().Set("X-Lab-Upstream-Status", "429")
					}
					w.WriteHeader(429)
					return
				}
				w.Header().Set("X-Lab-Admitted", "true")
				w.Header().Set("Content-Type", "text/event-stream")
				if mode != "first" {
					io.WriteString(w, textEvent)
				}
				w.(http.Flusher).Flush()
				switch mode {
				case "timeout", "idle", "first", "cancel":
					<-r.Context().Done()
				case "sse":
					io.WriteString(w, "data: {\"error\":{\"code\":\"bad\"}}\n\n"+doneEvent)
				case "empty-error":
					io.WriteString(w, "event: error\ndata:\n\n"+doneEvent)
				case "legacy":
					io.WriteString(w, "data: {\"object\":\"error\",\"message\":\"bad\"}\n\n"+doneEvent)
				}
			}))
			defer up.Close()
			c := config(t, up.URL)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "timeout":
				c.Deadlines.Total = 25 * time.Millisecond
			case "idle":
				c.Deadlines.Idle = 25 * time.Millisecond
			case "first":
				c.Deadlines.FirstText = 25 * time.Millisecond
			case "cancel":
				time.AfterFunc(25*time.Millisecond, cancel)
			}
			p := workload().Prompts[0]
			cl := client()
			defer cl.CloseIdleConnections()
			r := execute(ctx, cl, c, p, baseRecord(c, p, "test", 1, 0, false, true), time.Now())
			if r.Outcome == "success" || r.ErrorCode == "" || r.FinishedMS == nil {
				t.Fatalf("%+v", r)
			}
			if !strings.Contains(mode, "429") && mode != "first" && r.FirstTextMS == nil {
				t.Fatal("lost observed text")
			}
			expected := map[string]string{"timeout": "total_timeout", "idle": "idle_timeout", "first": "first_text_timeout", "cancel": "client_canceled", "sse": "sse_error", "empty-error": "sse_error", "legacy": "sse_error", "truncated": "truncated_stream", "gateway429": "gateway_rejected", "upstream429": "upstream_429", "direct429": "upstream_429"}[mode]
			if r.ErrorCode != expected {
				t.Fatalf("want %s got %+v", expected, r)
			}
			if mode == "upstream429" && r.Admission != "true" {
				t.Fatal("lost admitted upstream rejection")
			}
		})
	}
}
func TestInfinityAndPopulations(t *testing.T) {
	records := []Record{}
	for i := 0; i < 20; i++ {
		r := Record{Outcome: "success", Admission: "true", FirstByteMS: ptr(float64(i + 1)), FirstTextMS: ptr(float64(i + 1)), ScheduledFirstTextMS: ptr(float64(i + 2)), FinishedMS: ptr(float64(i + 5))}
		if i >= 18 {
			r.Outcome = "failure"
		}
		records = append(records, r)
	}
	records = append(records, Record{Outcome: "dropped", Admission: "unknown"})
	p := population(records, false)
	if p.Count != 21 || p.Successes != 18 || *p.MetricsMS["success_first_text"].P95.Value != 18 || !p.MetricsMS["penalised_first_text"].P95.Unbounded || p.MetricsMS["penalised_first_text"].P50.Unbounded {
		t.Fatalf("%+v", p)
	}
	a := population(records, true)
	if a.Count != 20 || !a.MetricsMS["penalised_completion"].P95.Unbounded {
		t.Fatal("admitted failures missing")
	}
	encoded, e := json.Marshal(p)
	if e != nil || !strings.Contains(string(encoded), `"value":null,"unbounded":true,"undefined":false`) {
		t.Fatalf("%s %v", encoded, e)
	}
	if !distribution(nil).P50.Undefined {
		t.Fatal("empty distribution must be undefined")
	}
	if !distribution([]float64{math.Inf(1)}).P50.Unbounded {
		t.Fatal("infinity lost")
	}
	if distribution(make([]float64, 999)).P99.Value != nil || distribution(make([]float64, 1000)).P99.Value == nil {
		t.Fatal("p99 sample threshold")
	}
}

type badSink struct{ syncErr bool }

func (b badSink) Write(p []byte) (int, error) {
	if b.syncErr {
		return len(p), nil
	}
	return 0, io.ErrClosedPipe
}
func (b badSink) Sync() error {
	if b.syncErr {
		return io.ErrClosedPipe
	}
	return nil
}
func TestWriteFailuresStopAndSurface(t *testing.T) {
	for _, b := range []badSink{{}, {true}} {
		if !errors.Is(writeRecord(b, Record{}), io.ErrClosedPipe) {
			t.Fatal("write failure hidden")
		}
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { success(w) }))
	defer up.Close()
	c := config(t, up.URL)
	c.Attempts = 100
	cl := client()
	defer cl.CloseIdleConnections()
	complete, e := runRepeat(context.Background(), cl, c, workload(), Schedule{}, "x", 1, time.Now(), badSink{})
	if e == nil || complete {
		t.Fatal("writer failure did not stop run")
	}
	if e := AtomicJSON(filepath.Join(t.TempDir(), "missing", "out.json"), 1); e == nil {
		t.Fatal("snapshot write failure hidden")
	}
}
func TestInterruptedAndTornEvidence(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, textEvent)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	c := config(t, up.URL)
	c.Attempts = 10
	c.Repeats = 3
	c.Drain = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(25*time.Millisecond, cancel)
	began := time.Now()
	dir, e := Run(ctx, c, workload(), Schedule{})
	if e == nil || time.Since(began) > time.Second {
		t.Fatalf("%v", e)
	}
	sum, e := Summarize(dir)
	if e != nil {
		t.Fatal(e)
	}
	if sum.State != "interrupted" || sum.Repeats[0].Complete || sum.Repeats[0].Counts["offered"] != 10 || sum.UnstartedRepeats != 2 || len(sum.AcrossCompleteRepeats) != 0 {
		t.Fatalf("%+v", sum)
	}
	path := filepath.Join(dir, "requests.jsonl")
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	f.WriteString(`{"schema_version":`)
	f.Close()
	sum, e = Summarize(dir)
	if e != nil || !sum.PartialFinalLine {
		t.Fatalf("partial line %v %v", sum.PartialFinalLine, e)
	}
	f, _ = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	f.WriteString("\n")
	f.Close()
	if _, e = Summarize(dir); e == nil {
		t.Fatal("interior malformed record accepted")
	}
}

type slowSink struct {
	mu      sync.Mutex
	records []Record
}

func (s *slowSink) Write(p []byte) (int, error) {
	time.Sleep(4 * time.Millisecond)
	var r Record
	json.Unmarshal(p, &r)
	s.mu.Lock()
	s.records = append(s.records, r)
	s.mu.Unlock()
	return len(p), nil
}
func (s *slowSink) Sync() error { return nil }
func TestWriterBackpressureRecordsSchedulingLag(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { success(w) }))
	defer up.Close()
	c := config(t, up.URL)
	c.Mode = "open"
	c.MaxInflight = 1
	c.Workers = 1
	schedule := Schedule{SchemaVersion: 1, WindowMS: 1}
	for i := 0; i < 20; i++ {
		schedule.Arrivals = append(schedule.Arrivals, Arrival{PromptID: "p"})
	}
	sink := &slowSink{}
	cl := client()
	defer cl.CloseIdleConnections()
	_, e := runRepeat(context.Background(), cl, c, workload(), schedule, "lag", 1, time.Now(), sink)
	if e != nil {
		t.Fatal(e)
	}
	observed := false
	for _, r := range sink.records {
		if r.SchedulingDelayMS != nil && *r.SchedulingDelayMS > 5 {
			observed = true
		}
	}
	if !observed {
		t.Fatal("writer-induced dispatch lag was hidden")
	}
}

func TestOpenDrainIsBoundedAndScheduleSaved(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, textEvent)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	c := config(t, up.URL)
	c.Mode = "open"
	c.Repeats = 3
	c.Drain = 20 * time.Millisecond
	s := Schedule{SchemaVersion: 1, WindowMS: 10, Arrivals: []Arrival{{OffsetMS: 0, PromptID: "p"}, {OffsetMS: 2, PromptID: "p"}}}
	began := time.Now()
	dir, e := Run(context.Background(), c, workload(), s)
	if e != nil {
		t.Fatal(e)
	}
	if time.Since(began) > time.Second || calls.Load() != 6 {
		t.Fatalf("unbounded drain or retries: %d calls", calls.Load())
	}
	records, _, e := ReadRecords(filepath.Join(dir, "requests.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range records {
		if r.ErrorCode != "drain_timeout" || r.FirstTextMS == nil {
			t.Fatalf("%+v", r)
		}
	}
	saved, e := LoadSchedule(filepath.Join(dir, "schedule.json"))
	if e != nil || len(saved.Arrivals) != 2 || saved.WindowMS != 10 {
		t.Fatal("schedule not preserved")
	}
	summary, e := Summarize(dir)
	if e != nil {
		t.Fatal(e)
	}
	aggregate := summary.AcrossCompleteRepeats["offered.penalised_first_text.p95"]
	if aggregate.Repeats != 3 || !aggregate.Median.Unbounded || !aggregate.Min.Unbounded || !aggregate.Max.Unbounded {
		t.Fatalf("infinite repeats lost: %+v", aggregate)
	}
}
func TestMissingRecordsStayInDenominator(t *testing.T) {
	m := Manifest{Config: Defaults()}
	rep := Repeat{Number: 1, Expected: 10, Complete: true, WindowMS: 100}
	records := []Record{{Outcome: "success", Admission: "true", FirstTextMS: ptr(1.0), FinishedMS: ptr(2.0)}}
	s := summarizeRepeat(m, rep, records)
	if s.Complete || s.Counts["offered"] != 10 || s.Counts["missing_records"] != 9 || s.Counts["admission_unknown"] != 9 || s.Counts["failed"] != 9 || s.KnownAdmitted.Count != 1 || !s.Offered.MetricsMS["penalised_first_text"].P50.Unbounded {
		t.Fatalf("%+v", s)
	}
}
func TestMaxDurationStopsClosedDispatch(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	c := config(t, up.URL)
	c.MaxDuration = 15 * time.Millisecond
	c.Drain = 15 * time.Millisecond
	c.Attempts = 10
	dir, e := Run(context.Background(), c, workload(), Schedule{})
	if e == nil {
		t.Fatal("run limit not reported")
	}
	sum, e := Summarize(dir)
	if e != nil {
		t.Fatal(e)
	}
	if sum.State != "failed" || sum.Repeats[0].Complete || sum.Repeats[0].Counts["offered"] != 10 || sum.Repeats[0].Counts["dropped"] < 7 {
		t.Fatalf("%+v", sum)
	}
}
func TestNoEOFDoesNotHoldRequest(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, textEvent+doneEvent)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	c := config(t, up.URL)
	cl := client()
	defer cl.CloseIdleConnections()
	p := workload().Prompts[0]
	began := time.Now()
	r := execute(context.Background(), cl, c, p, baseRecord(c, p, "test", 1, 0, false, true), time.Now())
	if r.Outcome != "success" || time.Since(began) > 100*time.Millisecond {
		t.Fatalf("terminal response blocked: %+v", r)
	}
}

func TestDispatchLimitDoesNotShortenNormalOpenDrain(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		time.Sleep(90 * time.Millisecond)
		success(w)
	}))
	defer up.Close()
	c := config(t, up.URL)
	c.Mode = "open"
	c.MaxDuration = 60 * time.Millisecond
	c.Drain = 100 * time.Millisecond
	schedule := Schedule{SchemaVersion: 1, WindowMS: 60, Arrivals: []Arrival{{OffsetMS: 0, PromptID: "p"}, {OffsetMS: 10, PromptID: "p"}}}
	dir, e := Run(context.Background(), c, workload(), schedule)
	if e != nil {
		t.Fatal(e)
	}
	s, e := Summarize(dir)
	if e != nil {
		t.Fatal(e)
	}
	if !s.Repeats[0].Complete || s.Repeats[0].Counts["success"] != 2 {
		t.Fatalf("%+v", s.Repeats[0])
	}
}

func TestTerminalErrorsDoNotWaitForEOFOrBecomeTimeouts(t *testing.T) {
	for _, status := range []int{429, 500, 200} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.Header().Set("X-Lab-Admitted", "true")
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(status)
				if status == 200 {
					io.WriteString(w, textEvent+"event: error\ndata: failure\n\n")
				}
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer up.Close()
			c := config(t, up.URL)
			c.Deadlines.FirstText = 40 * time.Millisecond
			cl := client()
			defer cl.CloseIdleConnections()
			p := workload().Prompts[0]
			began := time.Now()
			r := execute(context.Background(), cl, c, p, baseRecord(c, p, "test", 1, 0, false, true), time.Now())
			expected := map[int]string{429: "upstream_429", 500: "http_500", 200: "sse_error"}[status]
			if r.ErrorCode != expected || time.Since(began) > 100*time.Millisecond || r.Admission != "true" {
				t.Fatalf("%+v", r)
			}
		})
	}
}

type unwrittenFailureConn struct {
	net.Conn
	fail *atomic.Bool
}

func (c unwrittenFailureConn) Write(p []byte) (int, error) {
	if c.fail.Load() {
		return 0, io.ErrClosedPipe
	}
	return c.Conn.Write(p)
}
func TestNoTransportReplayAfterUnwrittenPOST(t *testing.T) {
	var calls, dials atomic.Int32
	var fail atomic.Bool
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); calls.Add(1); success(w) }))
	defer up.Close()
	c := config(t, up.URL)
	cl := client()
	defer cl.CloseIdleConnections()
	transport := cl.Transport.(*http.Transport)
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		return unwrittenFailureConn{Conn: conn, fail: &fail}, nil
	}
	p := workload().Prompts[0]
	first := execute(context.Background(), cl, c, p, baseRecord(c, p, "test", 1, 0, false, true), time.Now())
	if first.Outcome != "success" {
		t.Fatal("could not prime idle connection")
	}
	fail.Store(true)
	second := execute(context.Background(), cl, c, p, baseRecord(c, p, "test", 1, 1, false, true), time.Now())
	if second.Outcome != "failure" || second.ErrorCode != "transport_error" || calls.Load() != 1 || dials.Load() != 1 {
		t.Fatalf("POST was replayed: %+v server_calls=%d dials=%d", second, calls.Load(), dials.Load())
	}
}

func TestRejectionRatesAndSyntheticEvidenceStaySeparate(t *testing.T) {
	m := Manifest{Config: Defaults()}
	rep := Repeat{Number: 1, Expected: 4, Complete: true, WindowMS: 1000}
	records := []Record{
		{Outcome: "rejected", Admission: "false", ErrorCode: "gateway_rejected", FinishedMS: ptr(1.0)},
		{Outcome: "rejected", Admission: "true", ErrorCode: "upstream_429", UpstreamStatus: 429, FinishedMS: ptr(5.0)},
		{Outcome: "success", Admission: "true", FirstTextMS: ptr(2.0), FinishedMS: ptr(3.0)},
		{Outcome: "success", Admission: "true", FirstTextMS: ptr(2.0), FinishedMS: ptr(3.0)},
	}
	s := summarizeRepeat(m, rep, records)
	if *s.Rates["rejection_rate"].Value != .5 || *s.Rates["gateway_rejection_rate"].Value != .25 || *s.Rates["upstream_429_rate"].Value != .25 {
		t.Fatalf("rejection kinds conflated: %+v", s.Rates)
	}

	// A non-synthetic manifest still yields a synthetic summary when any saved
	// response carried X-Lab-Synthetic evidence.
	dir := t.TempDir()
	run := Manifest{SchemaVersion: Schema, RunID: "run", Config: Defaults(), Repeats: []Repeat{{Number: 1, Expected: 1, Complete: true, WindowMS: 1}}}
	if e := AtomicJSON(filepath.Join(dir, "manifest.json"), run); e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(Record{SchemaVersion: Schema, RunID: "run", Repeat: 1, RequestID: "a", Outcome: "success", Admission: "unknown", Synthetic: true})
	if e := os.WriteFile(filepath.Join(dir, "requests.jsonl"), append(b, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
	sum, e := Summarize(dir)
	if e != nil || !sum.Synthetic {
		t.Fatalf("synthetic response evidence dropped: %v %+v", e, sum)
	}
}

func TestDrainDefaultsToTotalDeadlineAndPoolCoversInflight(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { success(w) }))
	defer up.Close()
	c := config(t, up.URL)
	c.Drain = 0
	c.Attempts = 2
	c.MaxInflight = 700
	dir, e := Run(context.Background(), c, workload(), Schedule{})
	if e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if e != nil {
		t.Fatal(e)
	}
	var m Manifest
	if e = json.Unmarshal(b, &m); e != nil {
		t.Fatal(e)
	}
	if m.Config.Drain != c.Deadlines.Total {
		t.Fatalf("drain not resolved to total deadline: %v", m.Config.Drain)
	}
	if m.Transport["max_idle_connections_per_host"] != float64(700) || m.Transport["max_idle_connections"] != float64(700) {
		t.Fatalf("idle pool smaller than generator bound: %+v", m.Transport)
	}
	if Defaults().ResolveDrain() != stream.DefaultDeadlines().Total {
		t.Fatal("default drain should track the total deadline")
	}
}
