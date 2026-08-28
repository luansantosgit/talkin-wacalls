# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install git and ca-certificates (for HTTPS connections to WhatsApp)
RUN apk add --no-cache git ca-certificates

# Copy go mod files first for caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the server binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /server ./cmd/server

# Runtime stage
FROM alpine:3.20

WORKDIR /app

# Install ca-certificates for HTTPS
RUN apk add --no-cache ca-certificates tzdata

# Copy binary from builder
COPY --from=builder /server .

# Create directory for database
RUN mkdir -p /data

EXPOSE 8080

VOLUME ["/data"]

ENTRYPOINT ["./server"]
CMD ["-addr", ":8080", "-db", "/data/wacalls.db"]
