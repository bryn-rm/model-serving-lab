GO ?= go
export GOCACHE ?= /tmp/model-serving-lab-go-cache
.PHONY: check build mock-up mock-down gpu-up gpu-down
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
gpu-up:
	@echo 'Pending GPU session: image digest, compatibility and model revision must be resolved first.'; exit 1
gpu-down:
	@echo 'No GPU deployment configured in Phases 0–1.'
