package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"model-serving-lab/internal/bench"
)

func TestBenchConfigAndFlagOverrides(t *testing.T) {
	var calls atomic.Int32
	t.Setenv("BENCH_TEST_KEY", "test-secret")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-secret" {
			t.Error("config credential source ignored")
		}
		io.Copy(io.Discard, r.Body)
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer up.Close()
	root := t.TempDir()
	inputs := filepath.Join(root, "inputs")
	if code := runPrepare([]string{"--synthetic", "--output", inputs}); code != 0 {
		t.Fatal("prepare failed")
	}
	path := filepath.Join(root, "bench.json")
	settings := map[string]any{"url": up.URL, "target": "mock-direct", "workers": 2, "max_inflight": 4, "attempts_per_repeat": 3, "repeats": 1, "warmup_requests_per_repeat": 0, "workload": filepath.Join(inputs, "workload.json"), "output": filepath.Join(root, "runs"), "api_key_env": "BENCH_TEST_KEY", "environment": map[string]any{"topology": "test"}}
	if e := bench.AtomicJSON(path, settings); e != nil {
		t.Fatal(e)
	}
	if code := runBench([]string{"--config", path, "--attempts", "5"}); code != 0 {
		t.Fatal("config run failed")
	}
	if calls.Load() != 5 {
		t.Fatalf("flag override ignored: %d", calls.Load())
	}
	dirs, e := os.ReadDir(filepath.Join(root, "runs"))
	if e != nil || len(dirs) != 1 {
		t.Fatal("unique run not created")
	}
	b, e := os.ReadFile(filepath.Join(root, "runs", dirs[0].Name(), "manifest.json"))
	if e != nil {
		t.Fatal(e)
	}
	var m bench.Manifest
	if e = json.Unmarshal(b, &m); e != nil {
		t.Fatal(e)
	}
	if m.Config.Workers != 2 || m.Config.Attempts != 5 || m.Config.Warmup != 0 {
		t.Fatalf("config not preserved: %+v", m.Config)
	}
	// A literal credential field is intentionally unsupported in saved config.
	if e = os.WriteFile(path, []byte(`{"api_key":"should-not-be-saved"}`), 0600); e != nil {
		t.Fatal(e)
	}
	if code := runBench([]string{"--config=" + path}); code == 0 {
		t.Fatal("literal credential accepted")
	}
}
