package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"model-serving-lab/internal/bench"
)

type benchSettings struct {
	bench.Config
	Workload        string `json:"workload"`
	Schedule        string `json:"schedule"`
	EnvironmentFile string `json:"environment_file"`
	APIKeyEnv       string `json:"api_key_env"`
	Output          string `json:"output"`
}

func runBench(args []string) int {
	settings := benchSettings{Config: bench.Defaults(), Output: "results", APIKeyEnv: "LAB_API_KEY"}
	configPath := ""
	for i := 0; i < len(args); i++ {
		option := strings.TrimPrefix(args[i], "-")
		option = strings.TrimPrefix(option, "-")
		if option == "config" {
			if i+1 >= len(args) {
				return commandError(fmt.Errorf("--config requires a file"))
			}
			i++
			configPath = args[i]
		} else if path, ok := strings.CutPrefix(option, "config="); ok {
			configPath = path
		}
	}
	if configPath != "" {
		b, err := os.ReadFile(configPath)
		if err != nil {
			return commandError(err)
		}
		decoder := json.NewDecoder(bytes.NewReader(b))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&settings); err != nil {
			return commandError(err)
		}
		if err = decoder.Decode(new(any)); err != io.EOF {
			return commandError(fmt.Errorf("config must contain a single JSON object"))
		}
	}
	c := settings.Config
	c.OutputRoot = settings.Output
	f := flag.NewFlagSet("bench", flag.ContinueOnError)
	f.StringVar(&c.Mode, "mode", c.Mode, "closed or open")
	f.StringVar(&c.URL, "url", c.URL, "chat completions URL")
	f.StringVar(&c.Target, "target", c.Target, "mock-direct, mock-gateway, gpu-direct or gpu-gateway")
	f.StringVar(&c.OutputRoot, "output", c.OutputRoot, "parent directory for unique run directory")
	f.String("config", configPath, "saved benchmark JSON; command-line flags override its values")
	workload := f.String("workload", settings.Workload, "saved workload JSON (required)")
	schedule := f.String("schedule", settings.Schedule, "saved arrival schedule JSON (required for open mode)")
	environment := f.String("environment", settings.EnvironmentFile, "JSON environment/settings snapshot; unknown fields remain null")
	apiEnv := f.String("api-key-env", settings.APIKeyEnv, "environment variable holding target API key")
	defaultCap := -1
	if c.AdmissionCap != nil {
		defaultCap = *c.AdmissionCap
	}
	cap := f.Int("gateway-admission-cap", defaultCap, "recorded gateway cap, 0 disabled, -1 unknown")
	f.IntVar(&c.Workers, "workers", c.Workers, "fixed closed-loop workers")
	f.IntVar(&c.MaxInflight, "max-inflight", c.MaxInflight, "generator concurrency bound (must cover workers)")
	f.IntVar(&c.Attempts, "attempts", c.Attempts, "offered attempts per closed-loop repeat; open uses schedule length")
	f.IntVar(&c.Repeats, "repeats", c.Repeats, "measured repeats")
	f.IntVar(&c.Warmup, "warmup", c.Warmup, "sequential unmeasured warm-up requests before each repeat")
	f.DurationVar(&c.Deadlines.Total, "timeout", c.Deadlines.Total, "per-request total deadline")
	f.DurationVar(&c.Deadlines.FirstText, "first-text-timeout", c.Deadlines.FirstText, "per-request first-text/header deadline")
	f.DurationVar(&c.Deadlines.Idle, "idle-timeout", c.Deadlines.Idle, "per-request read idle deadline")
	f.DurationVar(&c.MaxDuration, "max-duration", c.MaxDuration, "maximum dispatch window per repeat")
	f.DurationVar(&c.Drain, "drain", c.Drain, "maximum drain after arrivals or interrupted dispatch; 0 uses the total request deadline")
	f.Float64Var(&c.ObjectiveMS, "objective-ms", c.ObjectiveMS, "first-text goodput objective in milliseconds")
	f.IntVar(&c.MaxEventBytes, "max-event-bytes", c.MaxEventBytes, "SSE event bound")
	f.StringVar(&c.MetricsURL, "metrics-url", c.MetricsURL, "private engine metrics URL; empty records unavailable")
	f.StringVar(&c.GatewayMetricsURL, "gateway-metrics-url", c.GatewayMetricsURL, "private gateway admin metrics URL; empty records unavailable")
	if err := f.Parse(args); err != nil {
		return 2
	}
	if *workload == "" || f.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "bench: --workload is required; unexpected positional arguments are not accepted")
		return 2
	}
	if c.Mode == "open" && *schedule == "" {
		fmt.Fprintln(os.Stderr, "bench: open mode requires --schedule")
		return 2
	}
	w, err := bench.LoadWorkload(*workload)
	if err != nil {
		return commandError(err)
	}
	var s bench.Schedule
	if *schedule != "" {
		s, err = bench.LoadSchedule(*schedule)
		if err != nil {
			return commandError(err)
		}
	}
	c.APIKey = os.Getenv(*apiEnv)
	if *cap >= 0 {
		c.AdmissionCap = cap
	} else {
		c.AdmissionCap = nil
	}
	metadata := map[string]any{}
	for _, key := range []string{"image_tag", "image_digest", "model_revision", "tokenizer_revision", "gpu", "driver", "topology", "max_num_seqs", "max_num_batched_tokens", "gpu_memory_utilization", "max_model_len", "dtype", "kv_cache_dtype", "cache_block_settings", "chunked_prefill", "prefix_caching", "native_rejection_controls"} {
		metadata[key] = nil
	}
	if err := json.Unmarshal(c.Environment, &metadata); err != nil {
		return commandError(err)
	}
	if *environment != "" {
		b, e := os.ReadFile(*environment)
		if e != nil {
			return commandError(e)
		}
		if e = json.Unmarshal(b, &metadata); e != nil {
			return commandError(e)
		}
	}
	c.Environment, err = json.Marshal(metadata)
	if err != nil {
		return commandError(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// The first signal starts a bounded drain. Restoring default handling lets
	// a second signal abort at once; synced JSONL records remain readable.
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-ctx.Done():
			stop()
			fmt.Fprintf(os.Stderr, "lab: interrupted; draining in-flight requests for up to %s (signal again to abort)\n", c.ResolveDrain())
		case <-finished:
		}
	}()
	dir, err := bench.Run(ctx, c, w, s)
	if dir != "" {
		fmt.Fprintln(os.Stdout, dir)
	}
	if err != nil {
		return commandError(err)
	}
	return 0
}

func runPrepare(args []string) int {
	f := flag.NewFlagSet("prepare", flag.ContinueOnError)
	output := f.String("output", "", "directory for workload.json and schedule.json (required)")
	model := f.String("model", "lab-model", "served model")
	prompt := f.String("prompt", "Explain streaming inference briefly.", "text prompt")
	attempts := f.Int("attempts", 200, "saved open-loop arrivals")
	rate := f.Float64("rate", 20, "fixed arrivals per second")
	tokens := f.Int("max-tokens", 128, "requested output length")
	synthetic := f.Bool("synthetic", false, "label workload and measurements synthetic")
	if err := f.Parse(args); err != nil {
		return 2
	}
	if *output == "" || *attempts < 1 || *tokens < 1 || *rate <= 0 || math.IsNaN(*rate) || math.IsInf(*rate, 0) || f.NArg() != 0 {
		return commandError(fmt.Errorf("prepare requires --output, positive attempts, rate and max-tokens"))
	}
	body, err := json.Marshal(map[string]any{"model": *model, "messages": []map[string]string{{"role": "user", "content": *prompt}}, "stream": true, "max_tokens": *tokens, "temperature": 0, "top_p": 1, "seed": 42, "ignore_eos": true, "stream_options": map[string]bool{"include_usage": true}})
	if err != nil {
		return commandError(err)
	}
	// LoadWorkload computes and verifies the body hash after saving.
	w := bench.Workload{SchemaVersion: bench.Schema, Synthetic: *synthetic, Prompts: []bench.Prompt{{ID: "prompt-1", Body: string(body)}}}
	s := bench.Schedule{SchemaVersion: bench.Schema, WindowMS: float64(*attempts) / *rate * 1000, Arrivals: []bench.Arrival{}}
	for i := 0; i < *attempts; i++ {
		s.Arrivals = append(s.Arrivals, bench.Arrival{OffsetMS: float64(i) / *rate * 1000, PromptID: "prompt-1"})
	}
	if err = os.MkdirAll(*output, 0700); err != nil {
		return commandError(err)
	}
	path := filepath.Join(*output, "workload.json")
	if err = bench.AtomicJSON(path, w); err != nil {
		return commandError(err)
	}
	w, err = bench.LoadWorkload(path)
	if err != nil {
		return commandError(err)
	}
	if err = bench.AtomicJSON(path, w); err != nil {
		return commandError(err)
	}
	if err = bench.AtomicJSON(filepath.Join(*output, "schedule.json"), s); err != nil {
		return commandError(err)
	}
	fmt.Fprintln(os.Stdout, *output)
	return 0
}
func runSummarize(args []string) int {
	f := flag.NewFlagSet("summarize", flag.ContinueOnError)
	run := f.String("run", "", "saved run directory (required)")
	output := f.String("output", "", "summary output JSON; default stdout")
	if err := f.Parse(args); err != nil {
		return 2
	}
	if *run == "" || f.NArg() != 0 {
		return commandError(fmt.Errorf("summarize requires --run"))
	}
	s, err := bench.Summarize(*run)
	if err != nil {
		return commandError(err)
	}
	if *output != "" {
		if err = bench.AtomicJSON(*output, s); err != nil {
			return commandError(err)
		}
	} else {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err = enc.Encode(s); err != nil {
			return commandError(err)
		}
	}
	return 0
}
func commandError(err error) int { fmt.Fprintln(os.Stderr, "lab:", err); return 1 }
