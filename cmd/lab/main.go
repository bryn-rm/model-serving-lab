package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"model-serving-lab/internal/smoke"
)

func main() { os.Exit(run()) }
func run() int {
	if len(os.Args) < 2 || os.Args[1] != "smoke" {
		fmt.Fprintln(os.Stderr, "usage: lab smoke [--url URL] [--output measurement.json]")
		return 2
	}
	o := smoke.Defaults()
	f := flag.NewFlagSet("smoke", flag.ContinueOnError)
	f.StringVar(&o.URL, "url", o.URL, "chat completions URL")
	f.StringVar(&o.Model, "model", o.Model, "model name")
	f.StringVar(&o.Prompt, "prompt", o.Prompt, "user text")
	f.StringVar(&o.APIKey, "api-key", os.Getenv("LAB_API_KEY"), "API key (or LAB_API_KEY)")
	f.DurationVar(&o.Total, "timeout", o.Total, "total deadline")
	f.DurationVar(&o.FirstText, "first-text-timeout", o.FirstText, "first text deadline")
	f.DurationVar(&o.Idle, "idle-timeout", o.Idle, "stream read idle deadline")
	f.IntVar(&o.MaxEvent, "max-event-bytes", o.MaxEvent, "SSE event bound")
	path := f.String("output", "measurement.json", "measurement JSON file")
	if err := f.Parse(os.Args[2:]); err != nil {
		return 2
	}
	file, err := os.Create(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer file.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := smoke.NewClient()
	defer client.CloseIdleConnections()
	obs, runErr := smoke.Run(ctx, client, o, os.Stdout)
	fmt.Fprintln(os.Stdout)
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err := enc.Encode(obs); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := file.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if runErr != nil {
		fmt.Fprintln(os.Stderr, runErr)
		return 1
	}
	return 0
}
