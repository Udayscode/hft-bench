# =========================================================
# BUILD STAGE
# =========================================================
FROM golang:1.25-alpine AS builder

# go-sqlite3 requires CGO
RUN apk add --no-cache gcc musl-dev

WORKDIR /app

# Dependency cache layer
COPY go.mod go.sum ./
RUN go mod download

# Pre-generated protobuf bindings are committed — no protoc needed
COPY . .

RUN CGO_ENABLED=1 GOOS=linux \
    go build -ldflags="-s -w" -o api-bin main.go

# =========================================================
# RUNTIME STAGE
# =========================================================
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/api-bin .

EXPOSE 8080

CMD ["./api-bin"]
