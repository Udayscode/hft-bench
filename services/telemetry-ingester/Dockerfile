FROM golang:1.25-alpine AS builder

# go-sqlite3 requires CGO
RUN apk add --no-cache gcc musl-dev

WORKDIR /app

# Better layer caching
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o ingester-bin main.go


FROM alpine:3.20

WORKDIR /app

# SSL certs for Kafka/Redpanda TLS later if needed
RUN apk add --no-cache ca-certificates

COPY --from=builder /app/ingester-bin .

VOLUME ["/app/data"]

EXPOSE 8081

CMD ["./ingester-bin"]
