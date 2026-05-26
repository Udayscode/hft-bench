# =========================================================
# BUILD STAGE
# =========================================================
FROM golang:1.25-alpine AS builder

# go-sqlite3 requires CGO — need gcc and musl-dev
RUN apk add --no-cache gcc musl-dev

WORKDIR /app

# Dependency cache layer
COPY go.mod go.sum ./
RUN go mod download

# Source copy
COPY . .

# CGO_ENABLED=1 required for go-sqlite3
RUN CGO_ENABLED=1 GOOS=linux \
    go build -ldflags="-s -w" -o leaderboard-bin main.go

# =========================================================
# RUNTIME STAGE
# =========================================================
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/leaderboard-bin .

# Frontend static files must be present for the file server
COPY --from=builder /app/frontend ./frontend

EXPOSE 3000

CMD ["./leaderboard-bin"]
