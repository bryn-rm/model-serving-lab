// Package bench implements durable, failure-aware streaming benchmarks.
package bench

import (
	"encoding/json"
	"model-serving-lab/internal/stream"
	"time"
)

const Schema = 1

type Prompt struct {
	ID     string `json:"prompt_id"`
	Body   string `json:"body"`
	SHA256 string `json:"sha256"`
}
type Workload struct {
	SchemaVersion int      `json:"schema_version"`
	Synthetic     bool     `json:"synthetic"`
	Prompts       []Prompt `json:"prompts"`
}
type Arrival struct {
	OffsetMS float64 `json:"offset_ms"`
	PromptID string  `json:"prompt_id"`
}
type Schedule struct {
	SchemaVersion int       `json:"schema_version"`
	WindowMS      float64   `json:"window_ms"`
	Arrivals      []Arrival `json:"arrivals"`
}

type Config struct {
	Mode          string           `json:"mode"`
	URL           string           `json:"url"`
	Target        string           `json:"target"`
	APIKey        string           `json:"-"`
	Workers       int              `json:"workers"`
	MaxInflight   int              `json:"max_inflight"`
	Attempts      int              `json:"attempts_per_repeat"`
	Repeats       int              `json:"repeats"`
	Warmup        int              `json:"warmup_requests_per_repeat"`
	Deadlines     stream.Deadlines `json:"deadlines_ns"`
	MaxEventBytes int              `json:"max_event_bytes"`
	MaxDuration   time.Duration    `json:"max_duration_ns"`
	Drain         time.Duration    `json:"drain_ns"` // 0 resolves to Deadlines.Total
	ObjectiveMS   float64          `json:"first_text_objective_ms"`
	AdmissionCap  *int             `json:"gateway_admission_cap"`
	MetricsURL    string           `json:"metrics_url"`
	OutputRoot    string           `json:"-"`
	Environment   json.RawMessage  `json:"environment"`
}

func Defaults() Config {
	return Config{Mode: "closed", URL: "http://127.0.0.1:8000/v1/chat/completions", Target: "mock-direct", Workers: 1, MaxInflight: 256, Attempts: 200, Repeats: 3, Warmup: 10, Deadlines: stream.DefaultDeadlines(), MaxEventBytes: 1 << 20, MaxDuration: 10 * time.Minute, ObjectiveMS: 1000, OutputRoot: "results", Environment: json.RawMessage(`{}`)}
}

type Record struct {
	SchemaVersion         int       `json:"schema_version"`
	RunID                 string    `json:"run_id"`
	Repeat                int       `json:"repeat"`
	RequestID             string    `json:"request_id"`
	PromptID              string    `json:"prompt_id"`
	BodySHA256            string    `json:"body_sha256"`
	Target                string    `json:"target"`
	Synthetic             bool      `json:"synthetic"`
	ScheduledOffsetMS     float64   `json:"scheduled_offset_ms"`
	DispatchOffsetMS      *float64  `json:"dispatch_offset_ms"`
	SchedulingDelayMS     *float64  `json:"scheduling_delay_ms"`
	FirstByteMS           *float64  `json:"first_byte_ms"`
	FirstTextMS           *float64  `json:"first_text_ms"`
	LastTextMS            *float64  `json:"last_text_ms"`
	DoneMS                *float64  `json:"done_ms"`
	FinishedMS            *float64  `json:"finished_ms"`
	ScheduledFirstTextMS  *float64  `json:"scheduled_first_text_ms"`
	GapsMS                []float64 `json:"text_chunk_gaps_ms"`
	PromptTokens          *int      `json:"prompt_tokens"`
	CompletionTokens      *int      `json:"completion_tokens"`
	RequestedOutputTokens *int      `json:"requested_output_tokens"`
	TextBytes             int       `json:"text_bytes"`
	FinishReason          string    `json:"finish_reason"`
	HTTPStatus            int       `json:"http_status"`
	UpstreamStatus        int       `json:"upstream_http_status"`
	ErrorCode             string    `json:"error_code"`
	Outcome               string    `json:"outcome"`
	Admission             string    `json:"admission"`
	GatewayRequestID      string    `json:"gateway_request_id"`
	ConnectionGets        int       `json:"connection_gets"`
	ConnectionsReused     int       `json:"connections_reused"`
	ConnectionsIdle       int       `json:"connections_idle"`
}

type Repeat struct {
	Number      int        `json:"repeat"`
	StartedUTC  time.Time  `json:"started_utc"`
	FinishedUTC *time.Time `json:"finished_utc"`
	WindowMS    float64    `json:"measurement_window_ms"`
	ElapsedMS   float64    `json:"elapsed_ms"`
	Expected    int        `json:"expected_attempts"`
	Complete    bool       `json:"complete"`
}
type Capture struct {
	Path       string `json:"path"`
	Status     string `json:"status"`
	HTTPStatus int    `json:"http_status"`
}
type Manifest struct {
	SchemaVersion   int                                   `json:"schema_version"`
	RunID           string                                `json:"run_id"`
	StartedUTC      time.Time                             `json:"started_utc"`
	FinishedUTC     *time.Time                            `json:"finished_utc"`
	State           string                                `json:"state"`
	Synthetic       bool                                  `json:"synthetic"`
	GitSHA          string                                `json:"git_sha"`
	GitDirty        bool                                  `json:"git_dirty"`
	GoVersion       string                                `json:"go_version"`
	Config          Config                                `json:"config"`
	WorkloadSHA256  string                                `json:"workload_sha256"`
	ScheduleSHA256  string                                `json:"schedule_sha256"`
	BodySHA256      map[string]string                     `json:"body_sha256"`
	RequestSettings map[string]map[string]json.RawMessage `json:"request_settings"`
	Transport       map[string]any                        `json:"transport"`
	Repeats         []Repeat                              `json:"repeats"`
	Captures        []Capture                             `json:"metric_captures"`
}

// MinIdleConns is the starting idle pool; it grows to cover MaxInflight so the
// generator never discards reusable connections below its own concurrency.
const MinIdleConns = 512

// ResolveDrain returns the effective drain. By default it equals the total
// request deadline, so arrivals late in an open-loop window get the same
// deadline policy as earlier ones instead of being censored by the drain.
func (c Config) ResolveDrain() time.Duration {
	if c.Drain == 0 {
		return c.Deadlines.Total
	}
	return c.Drain
}

func idleConns(c Config) int { return max(MinIdleConns, c.MaxInflight) }

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
func ptr[T any](v T) *T          { return &v }
