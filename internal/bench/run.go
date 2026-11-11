package bench

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"model-serving-lab/internal/stream"
)

func validate(c Config, w Workload, s Schedule) error {
	if c.Mode != "closed" && c.Mode != "open" {
		return errors.New("mode must be closed or open")
	}
	if c.Target != "mock-direct" && c.Target != "mock-gateway" && c.Target != "gpu-direct" && c.Target != "gpu-gateway" {
		return errors.New("invalid target label")
	}
	if err := safeURL(c.URL); err != nil {
		return err
	}
	if c.MetricsURL != "" {
		if err := safeURL(c.MetricsURL); err != nil {
			return err
		}
	}
	if c.Workers < 1 || c.MaxInflight < 1 || c.Workers > c.MaxInflight || c.Attempts < 1 || c.Repeats < 1 || c.Warmup < 0 || c.MaxEventBytes < 1 || c.MaxDuration <= 0 || c.Drain <= 0 || c.ObjectiveMS <= 0 || math.IsNaN(c.ObjectiveMS) || math.IsInf(c.ObjectiveMS, 0) {
		return errors.New("invalid load, limit, objective or duration")
	}
	if err := c.Deadlines.Validate(); err != nil {
		return err
	}
	if len(w.Prompts) == 0 || w.SchemaVersion != Schema {
		return errors.New("empty or unsupported workload")
	}
	if strings.HasPrefix(c.Target, "mock-") && !w.Synthetic {
		return errors.New("mock target requires a synthetic workload")
	}
	seen := map[string]bool{}
	for _, p := range w.Prompts {
		if p.ID == "" || seen[p.ID] || !json.Valid([]byte(p.Body)) || p.SHA256 != hash([]byte(p.Body)) {
			return errors.New("invalid workload or body hash")
		}
		seen[p.ID] = true
	}
	var env map[string]json.RawMessage
	if json.Unmarshal(c.Environment, &env) != nil || env == nil {
		return errors.New("environment must be JSON")
	}
	if c.Mode == "open" {
		if s.SchemaVersion != Schema || len(s.Arrivals) == 0 || s.WindowMS <= 0 || math.IsNaN(s.WindowMS) || math.IsInf(s.WindowMS, 0) || s.WindowMS > ms(c.MaxDuration) {
			return errors.New("invalid schedule window or schedule exceeds max-duration")
		}
		last := 0.0
		for _, a := range s.Arrivals {
			if !seen[a.PromptID] || math.IsNaN(a.OffsetMS) || math.IsInf(a.OffsetMS, 0) || a.OffsetMS < last || a.OffsetMS >= s.WindowMS {
				return errors.New("schedule must have ordered nonnegative offsets inside window and known prompts")
			}
			last = a.OffsetMS
		}
	}
	return nil
}
func safeURL(raw string) error {
	u, e := url.Parse(raw)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("URLs require http(s), host, and no credentials, query or fragment")
	}
	if _, e = http.NewRequest(http.MethodGet, raw, nil); e != nil {
		return errors.New("invalid URL")
	}
	return nil
}

// Run saves immutable inputs before dispatch and returns the directory even on failure.
func Run(ctx context.Context, c Config, w Workload, s Schedule) (dir string, result error) {
	c.Drain = c.ResolveDrain()
	if err := validate(c, w, s); err != nil {
		return "", err
	}
	if c.Mode == "open" {
		c.Attempts = len(s.Arrivals)
	}
	if err := os.MkdirAll(c.OutputRoot, 0700); err != nil {
		return "", err
	}
	id := time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + rand.Text()[:10]
	dir = filepath.Join(c.OutputRoot, id)
	if err := os.Mkdir(dir, 0700); err != nil {
		return "", err
	}
	workBytes, _ := json.Marshal(w)
	scheduleBytes, _ := json.Marshal(s)
	m := Manifest{SchemaVersion: Schema, RunID: id, StartedUTC: time.Now().UTC(), State: "running", Synthetic: w.Synthetic, Config: c, WorkloadSHA256: hash(workBytes), ScheduleSHA256: hash(scheduleBytes), GoVersion: runtime.Version(), Repeats: []Repeat{}, Captures: []Capture{}, Transport: map[string]any{}}
	m.BodySHA256 = map[string]string{}
	m.RequestSettings = map[string]map[string]json.RawMessage{}
	for _, p := range w.Prompts {
		m.BodySHA256[p.ID] = p.SHA256
		fields := map[string]json.RawMessage{}
		_ = json.Unmarshal([]byte(p.Body), &fields)
		settings := map[string]json.RawMessage{}
		for _, key := range []string{"model", "stream", "stream_options", "max_tokens", "max_completion_tokens", "temperature", "top_p", "seed", "ignore_eos", "n"} {
			if v, ok := fields[key]; ok {
				settings[key] = v
			}
		}
		m.RequestSettings[p.ID] = settings
	}
	transport := stream.NewTransport(idleConns(c), 0)
	m.Transport = map[string]any{"protocol": "HTTP/1.1", "max_idle_connections": transport.MaxIdleConns, "max_idle_connections_per_host": transport.MaxIdleConnsPerHost, "max_connections_per_host": transport.MaxConnsPerHost, "idle_timeout_seconds": transport.IdleConnTimeout.Seconds(), "disable_compression": transport.DisableCompression, "automatic_retries": false, "redirects": false}
	m.Transport["terminal_eof_drain_ms"] = 5
	m.Transport["terminal_eof_drain_max_bytes"] = 64 << 10

	if b, e := exec.Command("git", "rev-parse", "HEAD").Output(); e == nil {
		m.GitSHA = strings.TrimSpace(string(b))
	}
	if b, e := exec.Command("git", "status", "--porcelain").Output(); e == nil {
		m.GitDirty = len(b) > 0
	}
	save := func() error { return AtomicJSON(filepath.Join(dir, "manifest.json"), m) }
	if err := AtomicJSON(filepath.Join(dir, "workload.json"), w); err != nil {
		return dir, err
	}
	if err := AtomicJSON(filepath.Join(dir, "schedule.json"), s); err != nil {
		return dir, err
	}
	if err := save(); err != nil {
		return dir, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "requests.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return dir, err
	}
	warm, err := os.OpenFile(filepath.Join(dir, "warmup.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		f.Close()
		return dir, err
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	defer func() {
		result = errors.Join(result, f.Sync(), f.Close(), warm.Sync(), warm.Close())
		now := time.Now().UTC()
		m.FinishedUTC = &now
		m.State = "complete"
		if result != nil {
			m.State = "failed"
		}
		if ctx.Err() != nil {
			m.State = "interrupted"
			result = errors.Join(result, ctx.Err())
		}
		result = errors.Join(result, save())
		summary, e := Summarize(dir)
		if e == nil {
			e = AtomicJSON(filepath.Join(dir, "summary.json"), summary)
		}
		result = errors.Join(result, e)
	}()
	for repeat := 1; repeat <= c.Repeats; repeat++ {
		if ctx.Err() != nil {
			return dir, ctx.Err()
		}
		warmCtx, warmCancel := context.WithTimeout(ctx, c.MaxDuration)
		for i := 0; i < c.Warmup; i++ {
			p := w.Prompts[i%len(w.Prompts)]
			r := baseRecord(c, p, id, repeat, i, true, w.Synthetic)
			r = execute(warmCtx, client, c, p, r, time.Now())
			if err := writeRecord(warm, r); err != nil {
				warmCancel()
				return dir, err
			}
			if r.Outcome != "success" {
				warmCancel()
				return dir, fmt.Errorf("warm-up failed: %s", r.ErrorCode)
			}
		}
		warmCancel()
		capture, err := captureMetrics(ctx, client, c, dir, fmt.Sprintf("repeat-%d-before.prom", repeat))
		if err != nil {
			return dir, err
		}
		m.Captures = append(m.Captures, capture)
		rep := Repeat{Number: repeat, StartedUTC: time.Now().UTC(), Expected: c.Attempts}
		if c.Mode == "open" {
			rep.WindowMS = s.WindowMS
		}
		m.Repeats = append(m.Repeats, rep)
		if err := save(); err != nil {
			return dir, err
		}
		start := time.Now()
		complete, err := runRepeat(ctx, client, c, w, s, id, repeat, start, f)
		rep.ElapsedMS = ms(time.Since(start))
		if c.Mode == "closed" {
			rep.WindowMS = rep.ElapsedMS
		}
		now := time.Now().UTC()
		rep.FinishedUTC = &now
		rep.Complete = complete && err == nil
		m.Repeats[len(m.Repeats)-1] = rep
		if err != nil {
			return dir, err
		}
		capture, err = captureMetrics(ctx, client, c, dir, fmt.Sprintf("repeat-%d-after.prom", repeat))
		if err != nil {
			return dir, err
		}
		m.Captures = append(m.Captures, capture)
		if err := save(); err != nil {
			return dir, err
		}
		if !complete {
			return dir, errors.New("repeat interrupted or max-duration exceeded; partial evidence saved")
		}
	}
	return dir, nil
}
func baseRecord(c Config, p Prompt, id string, repeat, index int, warm, synthetic bool) Record {
	kind := "request"
	if warm {
		kind = "warmup"
	}
	return Record{SchemaVersion: Schema, RunID: id, Repeat: repeat, RequestID: fmt.Sprintf("%s-r%d-%s-%d", id, repeat, kind, index), PromptID: p.ID, BodySHA256: p.SHA256, Target: c.Target, Synthetic: synthetic, Admission: "unknown", GapsMS: []float64{}}
}

func runRepeat(parent context.Context, client *http.Client, c Config, w Workload, s Schedule, id string, repeat int, start time.Time, dst syncWriter) (bool, error) {
	dispatchCtx, stop := context.WithTimeout(parent, c.MaxDuration)
	defer stop()
	workCtx, cancelCause := context.WithCancelCause(context.Background())
	cancel := func() { cancelCause(errDrain) }
	defer cancel()
	allDone := make(chan struct{})
	go func() {
		select {
		case <-dispatchCtx.Done():
			select {
			case <-time.After(c.Drain):
				cancel()
			case <-allDone:
			}
		case <-allDone:
		}
	}()
	results := make(chan Record, c.MaxInflight)
	var complete atomic.Bool
	complete.Store(true)
	emitDrop := func(r Record, reason string) {
		r.Outcome = "dropped"
		r.Admission = "false"
		r.ErrorCode = reason
		results <- r
	}
	reason := func() string {
		if parent.Err() != nil {
			return "interrupted_before_dispatch"
		}
		return "run_limit_before_dispatch"
	}
	go func() {
		defer close(results)
		var wg sync.WaitGroup
		if c.Mode == "closed" {
			var next atomic.Int64
			for worker := 0; worker < c.Workers; worker++ {
				wg.Go(func() {
					for {
						i := int(next.Add(1)) - 1
						if i >= c.Attempts {
							return
						}
						p := w.Prompts[i%len(w.Prompts)]
						r := baseRecord(c, p, id, repeat, i, false, w.Synthetic)
						r.ScheduledOffsetMS = ms(time.Since(start))
						if dispatchCtx.Err() != nil {
							complete.Store(false)
							emitDrop(r, reason())
							continue
						}
						results <- execute(workCtx, client, c, p, r, start)
					}
				})
			}
			wg.Wait()
		} else {
			prompts := map[string]Prompt{}
			for _, p := range w.Prompts {
				prompts[p.ID] = p
			}
			slots := make(chan struct{}, c.MaxInflight)
			for i, a := range s.Arrivals {
				p := prompts[a.PromptID]
				r := baseRecord(c, p, id, repeat, i, false, w.Synthetic)
				r.ScheduledOffsetMS = a.OffsetMS
				delay := time.Until(start.Add(time.Duration(a.OffsetMS * float64(time.Millisecond))))
				if delay > 0 {
					timer := time.NewTimer(delay)
					select {
					case <-timer.C:
					case <-dispatchCtx.Done():
					}
					timer.Stop()
				}
				if dispatchCtx.Err() != nil {
					complete.Store(false)
					emitDrop(r, reason())
					continue
				}
				select {
				case slots <- struct{}{}:
					wg.Go(func() { defer func() { <-slots }(); results <- execute(workCtx, client, c, p, r, start) })
				default:
					emitDrop(r, "generator_capacity")
				}
			}
			// Normal end of arrivals also has a bounded drain, independent of request deadlines.
			timer := time.AfterFunc(c.Drain, cancel)
			wg.Wait()
			timer.Stop()
		}
		close(allDone)
	}()
	var writeErr error
	for r := range results {
		if writeErr == nil {
			if err := writeRecord(dst, r); err != nil {
				writeErr = err
				stop()
				cancel()
				complete.Store(false)
			}
		}
	}
	if parent.Err() != nil {
		complete.Store(false)
	}
	return complete.Load(), writeErr
}

func captureMetrics(ctx context.Context, client *http.Client, c Config, dir, name string) (Capture, error) {
	cap := Capture{Path: name, Status: "unavailable_no_metrics_url"}
	if c.MetricsURL == "" {
		return cap, AtomicJSON(filepath.Join(dir, name+".status.json"), cap)
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.MetricsURL, nil)
	resp, e := client.Do(req)
	if e != nil {
		cap.Status = "unavailable_transport"
		return cap, AtomicJSON(filepath.Join(dir, name+".status.json"), cap)
	}
	defer resp.Body.Close()
	cap.HTTPStatus = resp.StatusCode
	b, e := io.ReadAll(io.LimitReader(resp.Body, 8<<20+1))
	if e != nil || len(b) > 8<<20 {
		cap.Status = "unavailable_read"
	} else if resp.StatusCode != 200 {
		cap.Status = "unavailable_http"
	} else {
		cap.Status = "captured"
		f, e := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return cap, e
		}
		e = writeAll(f, b)
		e = errors.Join(e, f.Sync(), f.Close())
		if e != nil {
			return cap, e
		}
	}
	return cap, AtomicJSON(filepath.Join(dir, name+".status.json"), cap)
}
