GO ?= go
export GOCACHE ?= /tmp/model-serving-lab-go-cache
GPU_ENV ?= configs/gpu-pilot.env
GPU_COMPOSE = docker compose --env-file $(GPU_ENV) -f deploy/compose.gpu.yaml
.PHONY: check build mock-up mock-down gpu-up gpu-down gpu-config-check lab-docker
check:
	@unformatted="$$(gofmt -l $$(find cmd internal -name '*.go'))" || exit 1; test -z "$$unformatted" || (echo "Go formatting needed: $$unformatted"; exit 1)
	$(GO) vet ./...
	CC=clang CGO_ENABLED=1 $(GO) test -race ./...
build:
	$(GO) build -o bin/lab ./cmd/lab
	$(GO) build -o bin/gateway ./cmd/gateway
	$(GO) build -o bin/mockllm ./cmd/mockllm
mock-up:
	docker compose -f deploy/compose.cpu.yaml up --build -d
mock-down:
	docker compose -f deploy/compose.cpu.yaml down
# GPU stack (docs/gpu-runbook.md). Needs pins from scripts/gpu-preflight.sh
# --write and exported LAB_UPSTREAM_API_KEY / LAB_GATEWAY_API_KEY.
gpu-up:
	$(GPU_COMPOSE) up --build -d
gpu-down:
	$(GPU_COMPOSE) down
# Offline validation of the GPU config and helper scripts (no GPU needed).
gpu-config-check:
	for script in scripts/gpu-preflight.sh scripts/gpu-capture.sh scripts/gpu-cancel-check.sh scripts/demo-bench.sh; do bash -n "$$script" || exit 1; done
	python3 -m unittest discover -s scripts -p 'test_*.py'
	python3 scripts/check_gpu_config.py
# Build bin/lab in the pinned Go image, for hosts without a Go toolchain.
lab-docker:
	docker run --rm -u "$$(id -u):$$(id -g)" -v "$$PWD":/src -w /src -e HOME=/tmp -e GOCACHE=/tmp/gocache \
	  -e GOPATH=/tmp/go -e CGO_ENABLED=0 golang:1.27.3 go build -buildvcs=false -o bin/lab ./cmd/lab
