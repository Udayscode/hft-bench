package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

const (
	DefaultListenAddr = ":3001"
	DBFile            = "../telemetry-ingester/telemetry.db"
	ReadTimeout       = 10 * time.Second
	WriteTimeout      = 15 * time.Second
	IdleTimeout       = 60 * time.Second
	QueryTimeout      = 5 * time.Second
)

var startTime = time.Now()

// ─── Data Models ─────────────────────────────────────────────────────────────

type LeaderboardRow struct {
	Rank                  int     `json:"rank"`
	SubmissionID          string  `json:"submission_id"`
	P50Micros             int64   `json:"p50_micros"`
	P90Micros             int64   `json:"p90_micros"`
	P99Micros             int64   `json:"p99_micros"`
	SuccessRate           float64 `json:"success_rate"`
	Score                 float64 `json:"score"`
	TotalOrders           int64   `json:"total_orders"`
	CorrectnessViolations int64   `json:"correctness_violations"`
	TPS                   float64 `json:"tps"`
	Timestamp             string  `json:"timestamp"`
}

type ScoresResponse struct {
	Scores           []LeaderboardRow `json:"scores"`
	LastUpdated      string           `json:"last_updated"`
	ActiveBenchmarks int              `json:"active_benchmarks"`
}

type RunRecord struct {
	RunID       int64   `json:"run_id"`
	P50Micros   int64   `json:"p50_micros"`
	P90Micros   int64   `json:"p90_micros"`
	P99Micros   int64   `json:"p99_micros"`
	SuccessRate float64 `json:"success_rate"`
	Score       float64 `json:"score"`
	TPS         float64 `json:"tps"`
	Timestamp   string  `json:"timestamp"`
}

type SubmissionDetailResponse struct {
	SubmissionID string      `json:"submission_id"`
	Runs         []RunRecord `json:"runs"`
	BestScore    float64     `json:"best_score"`
	BestP99      int64       `json:"best_p99"`
}

// ─── Server ───────────────────────────────────────────────────────────────────

type LeaderboardServer struct {
	logger *log.Logger
	db     *sql.DB
}

type App struct {
	httpServer *http.Server
	logger     *log.Logger
	listenAddr string
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	logger := log.New(os.Stdout, "[leaderboard] ", log.LstdFlags|log.Lmicroseconds)

	db, err := initDB()
	if err != nil {
		logger.Fatalf("database initialization failed: %v", err)
	}
	defer db.Close()

	server := &LeaderboardServer{
		logger: logger,
		db:     db,
	}

	mux := http.NewServeMux()

	// Health
	mux.HandleFunc("/health", server.healthHandler)

	// Rich scores API
	mux.HandleFunc("/api/scores", server.handleScores)

	// Per-submission drill-down  (prefix match: /api/scores/<id>)
	mux.HandleFunc("/api/scores/", server.handleSubmissionDetail)

	// Legacy alias kept for backward compatibility
	mux.HandleFunc("/api/leaderboard", server.handleLegacyLeaderboard)

	// Static frontend
	fileServer := http.FileServer(http.Dir("./frontend"))
	mux.Handle("/", fileServer)

	port := os.Getenv("PORT")
	if port == "" {
		port = DefaultListenAddr
	} else if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	httpServer := &http.Server{
		Addr:           port,
		Handler:        withCORS(withRecovery(withLogging(logger, mux))),
		ReadTimeout:    ReadTimeout,
		WriteTimeout:   WriteTimeout,
		IdleTimeout:    IdleTimeout,
		MaxHeaderBytes: 1 << 20,
	}

	app := &App{httpServer: httpServer, logger: logger, listenAddr: port}
	app.run()
}

// ─── DB Init ──────────────────────────────────────────────────────────────────

func initDB() (*sql.DB, error) {
	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		dbUrl = "postgres://postgres:password@localhost:5432/hft_telemetry?sslmode=disable"
	}

	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(10 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

func (s *LeaderboardServer) healthHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":          "ok",
		"uptime_seconds":  int(time.Since(startTime).Seconds()),
	})
}

// GET /api/scores — full leaderboard with all metrics
func (s *LeaderboardServer) handleScores(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), QueryTimeout)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			submission_id,
			COALESCE(p50_micros, 0),
			COALESCE(p90_micros, p50_micros, 0),
			COALESCE(p99_micros, 0),
			COALESCE(success_rate, 0),
			COALESCE(total_orders, 0),
			COALESCE(timestamp, 0)
		FROM benchmark_metrics
		WHERE id IN (
			SELECT MAX(id)
			FROM benchmark_metrics
			GROUP BY submission_id
		)
	`)
	if err != nil {
		s.logger.Printf("scores query failed err=%v", err)
		writeError(w, http.StatusInternalServerError, "database query failed")
		return
	}
	defer rows.Close()

	type rawRun struct {
		submissionID string
		p50, p90, p99 int64
		successRate   float64
		totalOrders   int64
		timestamp     int64
	}

	// Group best run per submission
	bestRuns := make(map[string]LeaderboardRow)

	for rows.Next() {
		var run rawRun
		if err := rows.Scan(
			&run.submissionID,
			&run.p50,
			&run.p90,
			&run.p99,
			&run.successRate,
			&run.totalOrders,
			&run.timestamp,
		); err != nil {
			s.logger.Printf("row scan failed err=%v", err)
			continue
		}

		score := calculateScore(run.p50, run.p99, run.successRate)

		// Estimate TPS from latency (orders/second throughput proxy)
		tps := 0.0
		if run.p99 > 0 {
			tps = math.Min(float64(run.totalOrders)*1e6/float64(run.p99), 1_000_000)
		}

		var ts time.Time
		if run.timestamp > 200000000000 {
			ts = time.UnixMilli(run.timestamp)
		} else if run.timestamp > 0 {
			ts = time.Unix(run.timestamp, 0)
		} else {
			ts = time.Now()
		}
		tsStr := ts.UTC().Format(time.RFC3339)

		existing, found := bestRuns[run.submissionID]
		if !found || score > existing.Score || (score == existing.Score && run.p99 < existing.P99Micros) {
			bestRuns[run.submissionID] = LeaderboardRow{
				SubmissionID: run.submissionID,
				P50Micros:    run.p50,
				P90Micros:    run.p90,
				P99Micros:    run.p99,
				SuccessRate:  roundFloat(run.successRate, 4),
				Score:        score,
				TotalOrders:  run.totalOrders,
				TPS:          roundFloat(tps, 2),
				Timestamp:    tsStr,
			}
		}
	}

	if err := rows.Err(); err != nil {
		s.logger.Printf("row iteration failure err=%v", err)
		writeError(w, http.StatusInternalServerError, "result iteration failed")
		return
	}

	leaderboard := make([]LeaderboardRow, 0, len(bestRuns))
	for _, row := range bestRuns {
		leaderboard = append(leaderboard, row)
	}

	sort.Slice(leaderboard, func(i, j int) bool {
		if leaderboard[i].Score != leaderboard[j].Score {
			return leaderboard[i].Score > leaderboard[j].Score
		}
		return leaderboard[i].P99Micros < leaderboard[j].P99Micros
	})

	if len(leaderboard) > 100 {
		leaderboard = leaderboard[:100]
	}

	for i := range leaderboard {
		leaderboard[i].Rank = i + 1
	}

	writeJSON(w, http.StatusOK, ScoresResponse{
		Scores:           leaderboard,
		LastUpdated:      time.Now().UTC().Format(time.RFC3339),
		ActiveBenchmarks: 0, // could be populated from orchestrator in future
	})
}

// GET /api/scores/:id — all runs for a single submission
func (s *LeaderboardServer) handleSubmissionDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Extract submission_id from path: /api/scores/<id>
	submissionID := strings.TrimPrefix(r.URL.Path, "/api/scores/")
	submissionID = strings.TrimSpace(submissionID)
	if submissionID == "" {
		writeError(w, http.StatusBadRequest, "missing submission_id")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), QueryTimeout)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			id,
			COALESCE(p50_micros, 0),
			COALESCE(p90_micros, p50_micros, 0),
			COALESCE(p99_micros, 0),
			COALESCE(success_rate, 0),
			COALESCE(total_orders, 0),
			COALESCE(timestamp, 0)
		FROM benchmark_metrics
		WHERE submission_id = $1
		ORDER BY timestamp DESC
	`, submissionID)
	if err != nil {
		s.logger.Printf("submission detail query failed err=%v", err)
		writeError(w, http.StatusInternalServerError, "database query failed")
		return
	}
	defer rows.Close()

	var runs []RunRecord
	var bestScore float64
	var bestP99 int64 = math.MaxInt64

	for rows.Next() {
		var runID, p50, p90, p99, totalOrders int64
		var successRate float64
		var tsUnix int64

		if err := rows.Scan(&runID, &p50, &p90, &p99, &successRate, &totalOrders, &tsUnix); err != nil {
			continue
		}

		var tsVal time.Time
		if tsUnix > 200000000000 {
			tsVal = time.UnixMilli(tsUnix)
		} else if tsUnix > 0 {
			tsVal = time.Unix(tsUnix, 0)
		} else {
			tsVal = time.Now()
		}
		tsStr := tsVal.UTC().Format(time.RFC3339)

		score := calculateScore(p50, p99, successRate)
		tps := 0.0
		if p99 > 0 && totalOrders > 0 {
			tps = math.Min(float64(totalOrders)*1e6/float64(p99), 1_000_000)
		}

		runs = append(runs, RunRecord{
			RunID:       runID,
			P50Micros:   p50,
			P90Micros:   p90,
			P99Micros:   p99,
			SuccessRate: roundFloat(successRate, 4),
			Score:       score,
			TPS:         roundFloat(tps, 2),
			Timestamp:   tsStr,
		})

		if score > bestScore {
			bestScore = score
		}
		if p99 > 0 && p99 < bestP99 {
			bestP99 = p99
		}
	}

	if bestP99 == math.MaxInt64 {
		bestP99 = 0
	}

	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "result iteration failed")
		return
	}

	writeJSON(w, http.StatusOK, SubmissionDetailResponse{
		SubmissionID: submissionID,
		Runs:         runs,
		BestScore:    roundFloat(bestScore, 2),
		BestP99:      bestP99,
	})
}

// Legacy /api/leaderboard for backward compatibility
func (s *LeaderboardServer) handleLegacyLeaderboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), QueryTimeout)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			submission_id,
			COALESCE(p50_micros, 0),
			COALESCE(p99_micros, 0),
			COALESCE(success_rate, 0)
		FROM benchmark_metrics
		WHERE id IN (
			SELECT MAX(id)
			FROM benchmark_metrics
			GROUP BY submission_id
		)
	`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database query failed")
		return
	}
	defer rows.Close()

	bestRuns := make(map[string]LeaderboardRow)

	for rows.Next() {
		var submissionID string
		var p50, p99 int64
		var successRate float64

		if err := rows.Scan(&submissionID, &p50, &p99, &successRate); err != nil {
			continue
		}

		score := calculateScore(p50, p99, successRate)
		existing, found := bestRuns[submissionID]
		if !found || score > existing.Score || (score == existing.Score && p99 < existing.P99Micros) {
			bestRuns[submissionID] = LeaderboardRow{
				SubmissionID: submissionID,
				P99Micros:    p99,
				SuccessRate:  roundFloat(successRate*100, 2),
				Score:        score,
			}
		}
	}

	leaderboard := make([]LeaderboardRow, 0, len(bestRuns))
	for _, row := range bestRuns {
		leaderboard = append(leaderboard, row)
	}

	sort.Slice(leaderboard, func(i, j int) bool {
		if leaderboard[i].Score != leaderboard[j].Score {
			return leaderboard[i].Score > leaderboard[j].Score
		}
		return leaderboard[i].P99Micros < leaderboard[j].P99Micros
	})

	if len(leaderboard) > 100 {
		leaderboard = leaderboard[:100]
	}

	for i := range leaderboard {
		leaderboard[i].Rank = i + 1
	}

	writeJSON(w, http.StatusOK, leaderboard)
}

// ─── Score Calculation ────────────────────────────────────────────────────────

func calculateScore(p50Micros, p99Micros int64, successRate float64) float64 {
	if p99Micros <= 0 || p50Micros <= 0 {
		return 0
	}

	latencyScore := (400.0 / float64(p99Micros)) * 100.0
	if latencyScore > 100.0 {
		latencyScore = 100.0
	}

	tpsScore := (600.0 / float64(p99Micros)) * 100.0
	if tpsScore > 100.0 {
		tpsScore = 100.0
	}

	correctnessScore := successRate * 100.0

	stabilityScore := (float64(p50Micros) / float64(p99Micros)) * 100.0
	if stabilityScore > 100.0 {
		stabilityScore = 100.0
	}

	score := (0.40 * latencyScore) + (0.30 * tpsScore) + (0.20 * correctnessScore) + (0.10 * stabilityScore)
	return roundFloat(score, 2)
}

func roundFloat(value float64, precision int) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(value*ratio) / ratio
}

// ─── HTTP Utilities ───────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, "json encoding failure", http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withLogging(logger *log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Printf("request method=%s path=%s duration=%s remote=%s",
			r.Method, r.URL.Path, time.Since(start), r.RemoteAddr)
	})
}

func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// ─── App Lifecycle ────────────────────────────────────────────────────────────

func (a *App) run() {
	errChan := make(chan error, 1)

	go func() {
		a.logger.Printf("leaderboard server listening addr=%s", a.listenAddr)
		listener, err := net.Listen("tcp", a.listenAddr)
		if err != nil {
			errChan <- err
			return
		}
		errChan <- a.httpServer.Serve(listener)
	}()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errChan:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.Fatalf("http server failure: %v", err)
		}
	case sig := <-shutdownChan:
		a.logger.Printf("shutdown signal received signal=%s", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.httpServer.Shutdown(ctx); err != nil {
			a.logger.Printf("graceful shutdown failed: %v", err)
		}
	}

	a.logger.Println("leaderboard server shutdown complete")
}