# ---- Build stage ----
FROM golang:1.22-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git gcc musl-dev

# Copy go.mod and go.sum first for layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build
RUN CGO_ENABLED=1 go build -ldflags="-s -w -X github.com/ayoubzulfiqar/aerollm/internal/config.buildVersion=${VERSION:-dev}" \
    -o /bin/aerollm ./cmd/server

# ---- Final stage ----
FROM alpine:3.19

RUN apk add --no-cache ca-certificates libc6-compat

RUN addgroup -S app && adduser -S app -G app

COPY --from=builder /bin/aerollm /usr/local/bin/aerollm

USER app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1

ENTRYPOINT ["aerollm"]