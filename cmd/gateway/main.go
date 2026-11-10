// Command gateway runs the authenticating, admission-controlled streaming
// gateway in front of vLLM (or the synthetic mock).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"model-serving-lab/internal/gateway"
)

func main() { os.Exit(run()) }

func run() int {
	cfg := gateway.DefaultConfig()
	addr := flag.String("addr", "127.0.0.1:8080", "public listen address")
	adminAddr := flag.String("admin-addr", "127.0.0.1:8081", "private admin listen address (health, readiness, metrics)")
	grace := flag.Duration("shutdown-grace", 10*time.Second, "time in-flight streams get to finish on shutdown")
	flag.StringVar(&cfg.Upstream, "upstream", cfg.Upstream, "upstream base URL")
	flag.StringVar(&cfg.HealthPath, "upstream-health-path", cfg.HealthPath, "upstream readiness path")
	flag.StringVar(&cfg.Model, "model", cfg.Model, "the only accepted model name")
	flag.IntVar(&cfg.AdmissionCap, "admission-cap", cfg.AdmissionCap, "concurrent admitted requests; 0 disables gateway admission")
	flag.DurationVar(&cfg.RetryAfter, "retry-after", cfg.RetryAfter, "Retry-After sent with 429 (whole seconds)")
	flag.Int64Var(&cfg.MaxBodyBytes, "max-body-bytes", cfg.MaxBodyBytes, "request body bound")
	flag.IntVar(&cfg.MaxEventBytes, "max-event-bytes", cfg.MaxEventBytes, "upstream SSE event bound")
	flag.IntVar(&cfg.MaxOutputTokens, "max-output-tokens", cfg.MaxOutputTokens, "maximum max_tokens/max_completion_tokens")
	flag.DurationVar(&cfg.Deadlines.FirstText, "first-text-timeout", cfg.Deadlines.FirstText, "upstream headers and first text deadline")
	flag.DurationVar(&cfg.Deadlines.Idle, "idle-timeout", cfg.Deadlines.Idle, "upstream stream read idle deadline")
	flag.DurationVar(&cfg.Deadlines.Total, "timeout", cfg.Deadlines.Total, "total upstream request deadline")
	flag.DurationVar(&cfg.WriteTimeout, "write-timeout", cfg.WriteTimeout, "deadline for each downstream write/flush")
	flag.IntVar(&cfg.UpstreamIdleConns, "upstream-idle-conns", cfg.UpstreamIdleConns, "retained idle upstream connections")
	flag.Parse()
	// Credentials come only from the environment so they never appear in argv.
	cfg.GatewayKey = os.Getenv("LAB_GATEWAY_API_KEY")
	cfg.UpstreamKey = os.Getenv("LAB_UPSTREAM_API_KEY")

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	g, err := gateway.New(cfg, log)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gateway:", err, "(set LAB_GATEWAY_API_KEY; LAB_UPSTREAM_API_KEY is optional)")
		return 2
	}
	public, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	admin, err := net.Listen("tcp", *adminAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("gateway listening", "addr", public.Addr().String(), "admin_addr", admin.Addr().String(),
		"upstream", cfg.Upstream, "model", cfg.Model, "admission_cap", cfg.AdmissionCap,
		"first_text_timeout", cfg.Deadlines.FirstText.String(), "idle_timeout", cfg.Deadlines.Idle.String(),
		"total_timeout", cfg.Deadlines.Total.String(), "write_timeout", cfg.WriteTimeout.String(),
		"upstream_auth", cfg.UpstreamKey != "")
	if err := g.Serve(ctx, public, admin, *grace); err != nil {
		log.Error("serve", "error", err.Error())
		return 1
	}
	return 0
}
