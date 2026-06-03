# -----------------------------------------------------------------------------
# Stage 1: Builder
# Compiles the Go binaries.
# -----------------------------------------------------------------------------
FROM golang:1.24-alpine AS builder

WORKDIR /app
ARG GOPROXY=https://proxy.golang.org,direct
ARG GOSUMDB=sum.golang.org
ENV GOPROXY=${GOPROXY}
ENV GOSUMDB=${GOSUMDB}

# Git is required for fetching Go dependencies
RUN apk add --no-cache git

# 1. Cache Go dependencies before copying application source.
COPY go.mod go.sum ./
RUN go mod download

# 2. Copy Source Code
COPY . .

# 3. Build Microservices
# We build separate binaries: API Gateway, Storage Node, Tiering Worker, and Meta Service.
# CGO_ENABLED=0 ensures static linking .
# -ldflags="-s -w" strips debug information to reduce binary size.
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /usr/local/bin/api ./cmd/api
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /usr/local/bin/storage_node ./cmd/storage_node
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /usr/local/bin/tiering_worker ./cmd/tiering_worker
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /usr/local/bin/meta_service ./cmd/meta_service

# -----------------------------------------------------------------------------
# Stage 2: Release
# Creates the final, minimal runtime image.
# -----------------------------------------------------------------------------
FROM alpine:3.18

# CA Certificates are required for making HTTPS requests (if needed)
RUN apk add --no-cache ca-certificates

# Copy the compiled binaries from the builder stage
COPY --from=builder /usr/local/bin/api /usr/local/bin/api
COPY --from=builder /usr/local/bin/storage_node /usr/local/bin/storage_node
COPY --from=builder /usr/local/bin/tiering_worker /usr/local/bin/tiering_worker
COPY --from=builder /usr/local/bin/meta_service /usr/local/bin/meta_service

# Default entrypoint (Overridden by docker-compose.yaml 'command')
CMD ["/usr/local/bin/api"]
