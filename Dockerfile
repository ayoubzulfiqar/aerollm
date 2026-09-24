# ---- Build stage ----
FROM golang:1.26-alpine AS builder

ARG VERSION=dev

WORKDIR /build

# Copy go.mod and go.sum first for layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Static, reproducible build (all dependencies are pure Go).
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X github.com/ayoubzulfiqar/aerollm/internal/config.buildVersion=${VERSION}" \
    -o /bin/aerollm ./cmd/server

# ---- Final stage ----
FROM alpine:3.20

RUN apk add --no-cache ca-certificates wget \
    && addgroup -S app && adduser -S app -G app \
    && mkdir -p /etc/aerollm /data \
    && chown app:app /data

COPY --from=builder /bin/aerollm /usr/local/bin/aerollm
COPY config.yaml /etc/aerollm/config.yaml

WORKDIR /data
ENV AEROLLM_STATE_DIR=/data/aerollm-state \
    AEROLLM_LEARNING_DIR=/data/fine-tune-jobs

USER app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["aerollm"]
