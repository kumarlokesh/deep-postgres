# Targets: build, test, bench, lint, cover, docker-*, clean

BINARY    := deep-postgres
CMD_PATH  := ./cmd/deep-postgres
GO        := go
GOFLAGS   :=
LINTER    := golangci-lint

.PHONY: all build test test-race bench cover lint fmt vet tidy \
        docker-build docker-run docker-test clean help

all: build test lint

# ── Build ────────────────────────────────────────────────────────────────────

build:
	$(GO) build $(GOFLAGS) -o bin/$(BINARY) $(CMD_PATH)

# ── Test ─────────────────────────────────────────────────────────────────────

test:
	$(GO) test $(GOFLAGS) ./...

test-race:
	$(GO) test -race $(GOFLAGS) ./...

bench:
	$(GO) test -bench=. -benchmem $(GOFLAGS) ./...

cover:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# ── Quality ──────────────────────────────────────────────────────────────────

lint:
	$(LINTER) run ./...

fmt:
	$(GO) fmt ./...
	goimports -w .

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

# ── Docker ───────────────────────────────────────────────────────────────────

docker-build:
	docker compose build

docker-run:
	docker compose run --rm dev

docker-test:
	docker compose run --rm dev make test-race

docker-bench:
	docker compose run --rm dev make bench

# ── Cleanup ──────────────────────────────────────────────────────────────────

clean:
	rm -rf bin/ coverage.out coverage.html

# ── Help ─────────────────────────────────────────────────────────────────────

help:
	@echo "Targets:"
	@echo "  build        compile the binary to bin/$(BINARY)"
	@echo "  test         run all tests"
	@echo "  test-race    run all tests with race detector"
	@echo "  bench        run benchmarks"
	@echo "  cover        generate HTML coverage report"
	@echo "  lint         run golangci-lint"
	@echo "  fmt          run gofmt + goimports"
	@echo "  vet          run go vet"
	@echo "  tidy         run go mod tidy"
	@echo "  docker-build build Docker image"
	@echo "  docker-run   open shell in dev container"
	@echo "  docker-test  run tests inside container with race detector"
	@echo "  docker-bench run benchmarks inside container"
	@echo "  clean        remove build artifacts"
