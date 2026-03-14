# ── Builder stage ─────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN make build

# ── Dev stage (default for docker-compose dev target) ─────────────────────────
# Includes the full toolchain, linter, and race detector.
FROM golang:1.22-alpine AS dev

RUN apk add --no-cache git make curl

# Install golangci-lint
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh \
    | sh -s -- -b $(go env GOPATH)/bin v1.57.2

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download

# Source is mounted at runtime; this layer only provides the toolchain.
CMD ["make", "test-race"]

# ── Runtime stage (minimal image) ─────────────────────────────────────────────
FROM alpine:3.19 AS runtime

RUN apk add --no-cache ca-certificates
COPY --from=builder /workspace/bin/deep-postgres /usr/local/bin/deep-postgres

ENTRYPOINT ["deep-postgres"]
