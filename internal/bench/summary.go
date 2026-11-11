package bench

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
)

// Number makes infinity and absent data unambiguous in JSON.
type Number struct {
	Value     *float64 `json:"value"`
	Unbounded bool     `json:"unbounded"`
	Undefined bool     `json:"undefined"`
}

func number(v float64) Number {
	if math.IsInf(v, 1) {
		return Number{Unbounded: true}
	}
	if math.IsNaN(v) {
		return Number{Undefined: true}
	}
	return Number{Value: ptr(v)}
}
func (n Number) numeric() float64 {
	if n.Unbounded {
		return math.Inf(1)
	}
	if n.Value == nil {
		return math.NaN()
	}
	return *n.Value
}

type Distribution struct {
	Count int    `json:"count"`
	P50   Number `json:"p50"`
	P95   Number `json:"p95"`
	P99   Number `json:"p99"`
	Min   Number `json:"min"`
	Max   Number `json:"max"`
}

func distribution(values []float64) Distribution {
	sort.Float64s(values)
	d := Distribution{Count: len(values), P50: number(math.NaN()), P95: number(math.NaN()), P99: number(math.NaN()), Min: number(math.NaN()), Max: number(math.NaN())}
	if len(values) == 0 {
		return d
	}
	rank := func(p float64) Number { return number(values[int(math.Ceil(p*float64(len(values))))-1]) }
	d.P50 = rank(.5)
	d.P95 = rank(.95)
	if len(values) >= 1000 {
		d.P99 = rank(.99)
	}
	d.Min = number(values[0])
	d.Max = number(values[len(values)-1])
	return d
}

type Population struct {
	Count     int                     `json:"count"`
	Successes int                     `json:"successes"`
	MetricsMS map[string]Distribution `json:"metrics_ms"`
}
type RepeatSummary struct {
	Repeat        int                     `json:"repeat"`
	Complete      bool                    `json:"complete"`
	WindowMS      float64                 `json:"measurement_window_ms"`
	Counts        map[string]int          `json:"counts"`
	Rates         map[string]Number       `json:"rates"`
	Offered       Population              `json:"offered"`
	KnownAdmitted Population              `json:"known_admitted"`
	Diagnostics   map[string]Distribution `json:"diagnostics"`
}
type Aggregate struct {
	Repeats int    `json:"repeats"`
	Median  Number `json:"median"`
	Min     Number `json:"min"`
	Max     Number `json:"max"`
}
type Summary struct {
	SchemaVersion         int                  `json:"schema_version"`
	RunID                 string               `json:"run_id"`
	Synthetic             bool                 `json:"synthetic"`
	State                 string               `json:"state"`
	PartialFinalLine      bool                 `json:"partial_final_line"`
	MissingRecords        int                  `json:"missing_records"`
	UnstartedRepeats      int                  `json:"unstarted_repeats"`
	PopulationPolicy      string               `json:"population_policy"`
	Repeats               []RepeatSummary      `json:"repeats"`
	AcrossCompleteRepeats map[string]Aggregate `json:"across_complete_repeats"`
}

func population(records []Record, admittedOnly bool) Population {
	values := map[string][]float64{}
	for _, key := range []string{"first_byte", "first_text", "completion", "scheduled_first_text"} {
		values["success_"+key] = []float64{}
		values["penalised_"+key] = []float64{}
	}
	p := Population{MetricsMS: map[string]Distribution{}}
	for _, r := range records {
		if admittedOnly && r.Admission != "true" {
			continue
		}
		p.Count++
		success := r.Outcome == "success"
		if success {
			p.Successes++
		}
		timings := map[string]*float64{"first_byte": r.FirstByteMS, "first_text": r.FirstTextMS, "completion": r.FinishedMS, "scheduled_first_text": r.ScheduledFirstTextMS}
		for name, value := range timings {
			penalised := math.Inf(1)
			if success && value != nil {
				values["success_"+name] = append(values["success_"+name], *value)
				penalised = *value
			}
			values["penalised_"+name] = append(values["penalised_"+name], penalised)
		}
	}
	for key, v := range values {
		p.MetricsMS[key] = distribution(v)
	}
	return p
}
func summarizeRepeat(m Manifest, rep Repeat, records []Record) RepeatSummary {
	s := RepeatSummary{Repeat: rep.Number, Complete: rep.Complete && len(records) == rep.Expected, WindowMS: rep.WindowMS, Counts: map[string]int{}, Rates: map[string]Number{}, Diagnostics: map[string]Distribution{}}
	for _, key := range []string{"offered", "dispatched", "admitted", "admission_false", "admission_unknown", "success", "rejected", "gateway_rejected", "upstream_429", "failed", "dropped", "objective_met", "usage_present", "output_length_deviation", "connection_gets", "connections_reused", "connections_idle", "missing_records"} {
		s.Counts[key] = 0
	}
	diagnostics := map[string][]float64{"scheduling_delay_ms": {}, "prompt_tokens": {}, "completion_tokens": {}, "text_bytes": {}, "text_chunk_gaps_ms": {}, "rejection_latency_ms": {}}
	inferredWindow := 0.0
	for _, r := range records {
		s.Counts["offered"]++
		if r.DispatchOffsetMS != nil {
			s.Counts["dispatched"]++
		}
		switch r.Admission {
		case "true":
			s.Counts["admitted"]++
		case "false":
			s.Counts["admission_false"]++
		default:
			s.Counts["admission_unknown"]++
		}
		if r.Outcome != "failure" {
			s.Counts[r.Outcome]++
		}
		if r.Outcome == "failure" {
			s.Counts["failed"]++
		}
		if r.ErrorCode == "gateway_rejected" {
			s.Counts["gateway_rejected"]++
		}
		if r.ErrorCode == "upstream_429" {
			s.Counts["upstream_429"]++
		}
		if r.Outcome == "success" {
			first := r.FirstTextMS
			if m.Config.Mode == "open" {
				first = r.ScheduledFirstTextMS
			}
			if first != nil && *first <= m.Config.ObjectiveMS {
				s.Counts["objective_met"]++
			}
		}
		if r.SchedulingDelayMS != nil {
			diagnostics["scheduling_delay_ms"] = append(diagnostics["scheduling_delay_ms"], *r.SchedulingDelayMS)
		}
		if r.PromptTokens != nil && r.CompletionTokens != nil {
			s.Counts["usage_present"]++
		}
		if r.PromptTokens != nil {
			diagnostics["prompt_tokens"] = append(diagnostics["prompt_tokens"], float64(*r.PromptTokens))
		}
		if r.CompletionTokens != nil {
			diagnostics["completion_tokens"] = append(diagnostics["completion_tokens"], float64(*r.CompletionTokens))
			if r.RequestedOutputTokens != nil && *r.CompletionTokens != *r.RequestedOutputTokens {
				s.Counts["output_length_deviation"]++
			}
		}
		if r.DispatchOffsetMS != nil {
			diagnostics["text_bytes"] = append(diagnostics["text_bytes"], float64(r.TextBytes))
		}
		diagnostics["text_chunk_gaps_ms"] = append(diagnostics["text_chunk_gaps_ms"], r.GapsMS...)
		if r.Outcome == "rejected" && r.FinishedMS != nil {
			diagnostics["rejection_latency_ms"] = append(diagnostics["rejection_latency_ms"], *r.FinishedMS)
		}
		s.Counts["connection_gets"] += r.ConnectionGets
		s.Counts["connections_reused"] += r.ConnectionsReused
		s.Counts["connections_idle"] += r.ConnectionsIdle
		if r.DispatchOffsetMS != nil && r.FinishedMS != nil {
			inferredWindow = max(inferredWindow, *r.DispatchOffsetMS+*r.FinishedMS)
		}
	}
	// Missing durable records are conservative failures, never quietly removed
	// from a started repeat's offered denominator after a crash/write failure.
	s.Counts["missing_records"] = max(0, rep.Expected-len(records))
	populationRecords := append([]Record(nil), records...)
	for i := 0; i < s.Counts["missing_records"]; i++ {
		populationRecords = append(populationRecords, Record{Outcome: "failure", Admission: "unknown"})
	}
	s.Counts["offered"] += s.Counts["missing_records"]
	s.Counts["failed"] += s.Counts["missing_records"]
	s.Counts["admission_unknown"] += s.Counts["missing_records"]
	if s.WindowMS == 0 {
		s.WindowMS = inferredWindow
	}
	for key, v := range diagnostics {
		s.Diagnostics[key] = distribution(v)
	}
	ratio := func(n, d int) Number {
		if d == 0 {
			return number(math.NaN())
		}
		return number(float64(n) / float64(d))
	}
	for name, count := range map[string]string{"success_rate": "success", "rejection_rate": "rejected", "failure_rate": "failed", "drop_rate": "dropped", "objective_compliance_rate": "objective_met", "admission_unknown_rate": "admission_unknown", "gateway_rejection_rate": "gateway_rejected", "upstream_429_rate": "upstream_429"} {
		s.Rates[name] = ratio(s.Counts[count], s.Counts["offered"])
	}
	s.Rates["usage_coverage_dispatched"] = ratio(s.Counts["usage_present"], s.Counts["dispatched"])
	s.Rates["connection_reuse_fraction"] = ratio(s.Counts["connections_reused"], s.Counts["connection_gets"])
	s.Rates["goodput_requests_per_second"] = number(math.NaN())
	if s.WindowMS > 0 {
		s.Rates["goodput_requests_per_second"] = number(float64(s.Counts["objective_met"]) / (s.WindowMS / 1000))
	}
	s.Offered = population(populationRecords, false)
	s.KnownAdmitted = population(records, true)
	s.Rates["known_admitted_success_rate"] = ratio(s.KnownAdmitted.Successes, s.KnownAdmitted.Count)
	return s
}

// Summarize uses only saved evidence and never rewrites raw request records.
func Summarize(dir string) (Summary, error) {
	var m Manifest
	b, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if e != nil {
		return Summary{}, e
	}
	if e = json.Unmarshal(b, &m); e != nil {
		return Summary{}, e
	}
	if m.SchemaVersion != Schema {
		return Summary{}, errors.New("unsupported manifest schema")
	}
	records, partial, e := ReadRecords(filepath.Join(dir, "requests.jsonl"))
	if e != nil {
		return Summary{}, e
	}
	byRepeat := map[int][]Record{}
	seen := map[string]bool{}
	known := map[int]bool{}
	syntheticEvidence := m.Synthetic
	for _, r := range m.Repeats {
		known[r.Number] = true
	}
	for _, r := range records {
		if r.RunID != m.RunID || !known[r.Repeat] || seen[r.RequestID] || r.RequestID == "" {
			return Summary{}, errors.New("record identity does not match manifest or duplicate request ID")
		}
		if r.Outcome != "success" && r.Outcome != "failure" && r.Outcome != "rejected" && r.Outcome != "dropped" {
			return Summary{}, fmt.Errorf("invalid outcome %q", r.Outcome)
		}
		if r.Admission != "true" && r.Admission != "false" && r.Admission != "unknown" {
			return Summary{}, errors.New("invalid admission evidence")
		}
		seen[r.RequestID] = true
		syntheticEvidence = syntheticEvidence || r.Synthetic
		byRepeat[r.Repeat] = append(byRepeat[r.Repeat], r)
	}
	s := Summary{SchemaVersion: Schema, RunID: m.RunID, Synthetic: syntheticEvidence, State: m.State, PartialFinalLine: partial, UnstartedRepeats: m.Config.Repeats - len(m.Repeats), PopulationPolicy: "started repeats use frozen expected attempts; missing records penalised, unstarted repeats excluded; aggregates use complete repeats only", Repeats: []RepeatSummary{}, AcrossCompleteRepeats: map[string]Aggregate{}}
	all := map[string][]float64{}
	add := func(key string, n Number) {
		v := n.numeric()
		if _, ok := all[key]; !ok {
			all[key] = []float64{}
		}
		if !math.IsNaN(v) {
			all[key] = append(all[key], v)
		}
	}
	for _, rep := range m.Repeats {
		if len(byRepeat[rep.Number]) > rep.Expected {
			return Summary{}, errors.New("more records than planned attempts")
		}
		rs := summarizeRepeat(m, rep, byRepeat[rep.Number])
		s.MissingRecords += rs.Counts["missing_records"]
		s.Repeats = append(s.Repeats, rs)
		if !rs.Complete {
			continue
		}
		for key, n := range rs.Counts {
			add("counts."+key, number(float64(n)))
		}
		for key, n := range rs.Rates {
			add("rates."+key, n)
		}
		addDist := func(prefix string, ds map[string]Distribution) {
			for key, d := range ds {
				for quant, n := range map[string]Number{"p50": d.P50, "p95": d.P95, "p99": d.P99, "min": d.Min, "max": d.Max} {
					add(prefix+"."+key+"."+quant, n)
				}
			}
		}
		addDist("offered", rs.Offered.MetricsMS)
		addDist("known_admitted", rs.KnownAdmitted.MetricsMS)
		addDist("diagnostics", rs.Diagnostics)
	}
	for key, values := range all {
		sort.Float64s(values)
		n := len(values)
		if n == 0 {
			s.AcrossCompleteRepeats[key] = Aggregate{Median: number(math.NaN()), Min: number(math.NaN()), Max: number(math.NaN())}
			continue
		}
		mid := values[n/2]
		if n%2 == 0 {
			mid = (values[n/2-1] + values[n/2]) / 2
		}
		s.AcrossCompleteRepeats[key] = Aggregate{Repeats: n, Median: number(mid), Min: number(values[0]), Max: number(values[n-1])}
	}
	return s, nil
}
