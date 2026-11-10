package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"model-serving-lab/internal/mockllm"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8000", "listen address")
	fixture := flag.String("fixture", "testdata/streams/synthetic-success.sse", "synthetic SSE fixture")
	delay := flag.Duration("delay", 40*time.Millisecond, "delay before each event")
	fragment := flag.Int("fragment-bytes", 0, "flush fragments of this size (0: event)")
	flag.Parse()
	if *delay < 0 || *fragment < 0 {
		fmt.Fprintln(os.Stderr, "delay and fragment size must be nonnegative")
		os.Exit(2)
	}
	data, err := os.ReadFile(*fixture)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Addr: *addr, Handler: (mockllm.Server{Fixture: data, Delay: *delay, FragmentBytes: *fragment, APIKey: os.Getenv("LAB_UPSTREAM_API_KEY")}).Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if server.Shutdown(drain) != nil {
			server.Close()
		}
	}()
	fmt.Fprintf(os.Stderr, "synthetic mock listening on %s\n", *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	<-drained
}
