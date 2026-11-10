package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Outcome labels are a fixed, low-cardinality set.
const (
	outcomeSuccess          = "success"
	outcomeUnauthorized     = "unauthorized"
	outcomeInvalid          = "invalid_request"
	outcomeRejected         = "rejected"
	outcomeUpstreamDown     = "upstream_unavailable"
	outcomeUpstreamHTTP     = "upstream_http_error"
	outcomeUpstreamError    = "upstream_error"
	outcomeTruncated        = "stream_truncated"
	outcomeStreamError      = "stream_error"
	outcomeClientCanceled   = "client_canceled"
	outcomeClientWriteError = "client_write_error"
	outcomeShutdown         = "shutdown"
	// Deadline outcomes use stream.Reason names: first_text_timeout,
	// idle_timeout and total_timeout.
)

type metrics struct {
	registry  *prometheus.Registry
	requests  *prometheus.CounterVec
	inflight  prometheus.Gauge
	capacity  prometheus.Gauge
	firstText prometheus.Histogram
	duration  *prometheus.HistogramVec
	upstream  *prometheus.CounterVec
}

func newMetrics(admissionCap int) *metrics {
	latency := []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300}
	m := &metrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lab_gateway_requests_total",
			Help: "Chat requests by terminal outcome and admission decision.",
		}, []string{"outcome", "admitted"}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lab_gateway_inflight_requests",
			Help: "Admitted requests currently holding gateway capacity.",
		}),
		capacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lab_gateway_admission_cap",
			Help: "Configured admission cap; 0 means gateway admission is disabled.",
		}),
		firstText: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "lab_gateway_first_text_seconds",
			Help:    "Gateway receipt to first upstream text event, admitted requests with text.",
			Buckets: latency,
		}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "lab_gateway_request_duration_seconds",
			Help:    "Gateway receipt to terminal outcome.",
			Buckets: latency,
		}, []string{"outcome"}),
		upstream: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lab_gateway_upstream_connections_total",
			Help: "Upstream connections obtained for requests, by reuse.",
		}, []string{"reused"}),
	}
	m.capacity.Set(float64(admissionCap))
	m.registry.MustRegister(m.requests, m.inflight, m.capacity, m.firstText, m.duration, m.upstream,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}
