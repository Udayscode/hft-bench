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
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	ListenAddr = ":3000"

	DBFile = "../telemetry-ingester/telemetry.db"

	ReadTimeout = 10 * time.Second

	WriteTimeout = 15 * time.Second

	IdleTimeout = 60 * time.Second

	QueryTimeout = 5 * time.Second
)

type LeaderboardRow struct {
	Rank int `json:"rank"`

	SubmissionID string `json:"submission_id"`

	P99Micros int64 `json:"p99_micros"`

	SuccessRate float64 `json:"success_rate"`

	Score float64 `json:"score"`
}

type LeaderboardServer struct {
	logger *log.Logger

	db *sql.DB
}

type App struct {
	httpServer *http.Server

	logger *log.Logger
}

func main() {

	logger := log.New(
		os.Stdout,
		"[leaderboard] ",
		log.LstdFlags|log.Lmicroseconds,
	)

	db, err := initDB()
	if err != nil {

		logger.Fatalf(
			"database initialization failed: %v",
			err,
		)
	}

	defer db.Close()

	server := &LeaderboardServer{
		logger: logger,
		db:     db,
	}

	mux := http.NewServeMux()

	mux.HandleFunc(
		"/health",
		healthHandler,
	)

	mux.HandleFunc(
		"/api/leaderboard",
		server.getLeaderboardData,
	)

	fileServer := http.FileServer(
		http.Dir("./frontend"),
	)

	mux.Handle(
		"/",
		fileServer,
	)

	httpServer := &http.Server{
		Addr: ListenAddr,

		Handler:
			withRecovery(
				withLogging(
					logger,
					mux,
				),
			),

		ReadTimeout:
			ReadTimeout,

		WriteTimeout:
			WriteTimeout,

		IdleTimeout:
			IdleTimeout,

		MaxHeaderBytes:
			1 << 20,
	}

	app := &App{
		httpServer: httpServer,
		logger:     logger,
	}

	app.run()
}

func initDB() (*sql.DB, error) {

	dbPath := DBFile
	if v := os.Getenv("DATABASE_PATH"); v != "" {
		dbPath = v
	}

	db, err := sql.Open(
		"sqlite3",
		dbPath,
	)

	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(5)

	db.SetMaxIdleConns(5)

	db.SetConnMaxLifetime(
		10 * time.Minute,
	)

	if err := db.Ping(); err != nil {

		return nil, err
	}

	return db, nil
}

func (a *App) run() {

	errChan :=
		make(chan error, 1)

	go func() {

		a.logger.Printf(
			"leaderboard server listening addr=%s",
			ListenAddr,
		)

		listener, err :=
			net.Listen(
				"tcp",
				ListenAddr,
			)

		if err != nil {

			errChan <- err

			return
		}

		errChan <- a.httpServer.Serve(
			listener,
		)
	}()

	shutdownChan :=
		make(chan os.Signal, 1)

	signal.Notify(
		shutdownChan,
		os.Interrupt,
		syscall.SIGTERM,
	)

	select {

	case err := <-errChan:

		if err != nil &&
			!errors.Is(
				err,
				http.ErrServerClosed,
			) {

			a.logger.Fatalf(
				"http server failure: %v",
				err,
			)
		}

	case sig := <-shutdownChan:

		a.logger.Printf(
			"shutdown signal received signal=%s",
			sig.String(),
		)

		ctx, cancel :=
			context.WithTimeout(
				context.Background(),
				10*time.Second,
			)

		defer cancel()

		if err := a.httpServer.Shutdown(
			ctx,
		); err != nil {

			a.logger.Printf(
				"graceful shutdown failed: %v",
				err,
			)
		}
	}

	a.logger.Println(
		"leaderboard server shutdown complete",
	)
}

func (s *LeaderboardServer) getLeaderboardData(
	w http.ResponseWriter,
	r *http.Request,
) {

	if r.Method != http.MethodGet {

		writeError(
			w,
			http.StatusMethodNotAllowed,
			"method not allowed",
		)

		return
	}

	ctx, cancel :=
		context.WithTimeout(
			r.Context(),
			QueryTimeout,
		)

	defer cancel()

	rows, err := s.db.QueryContext(
		ctx,
		`
		SELECT
			submission_id,
			p50_micros,
			p99_micros,
			success_rate
		FROM benchmark_metrics
		`,
	)

	if err != nil {

		s.logger.Printf(
			"leaderboard query failed err=%v",
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"database query failed",
		)

		return
	}

	defer rows.Close()

	bestRuns := make(map[string]LeaderboardRow)

	for rows.Next() {
		var submissionID string
		var p50 int64
		var p99 int64
		var successRate float64

		if err := rows.Scan(
			&submissionID,
			&p50,
			&p99,
			&successRate,
		); err != nil {
			s.logger.Printf(
				"row scan failed err=%v",
				err,
			)
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

	if err := rows.Err(); err != nil {

		s.logger.Printf(
			"row iteration failure err=%v",
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"result iteration failed",
		)

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

	writeJSON(
		w,
		http.StatusOK,
		leaderboard,
	)
}

func calculateScore(
	p50Micros int64,
	p99Micros int64,
	successRate float64,
) float64 {

	if p99Micros <= 0 || p50Micros <= 0 {
		return 0
	}

	// 1. Latency score: 100 points if p99 <= 400us, scaling down proportionally.
	latencyScore := (400.0 / float64(p99Micros)) * 100.0
	if latencyScore > 100.0 {
		latencyScore = 100.0
	}

	// 2. TPS score: estimated throughput, 100 points if p99 <= 600us (corresponds to high throughput).
	tpsScore := (600.0 / float64(p99Micros)) * 100.0
	if tpsScore > 100.0 {
		tpsScore = 100.0
	}

	// 3. Correctness score: success rate of order matching (percentage).
	correctnessScore := successRate * 100.0

	// 4. Stability score: ratio of median (p50) to p99 latency.
	stabilityScore := (float64(p50Micros) / float64(p99Micros)) * 100.0
	if stabilityScore > 100.0 {
		stabilityScore = 100.0
	}

	// Composite Score: 40% latency, 30% TPS, 20% correctness, 10% stability
	score := (0.40 * latencyScore) + (0.30 * tpsScore) + (0.20 * correctnessScore) + (0.10 * stabilityScore)

	return roundFloat(score, 2)
}

func roundFloat(
	value float64,
	precision int,
) float64 {

	ratio :=
		math.Pow(
			10,
			float64(
				precision,
			),
		)

	return math.Round(
		value*ratio,
	) / ratio
}

func healthHandler(
	w http.ResponseWriter,
	_ *http.Request,
) {

	writeJSON(
		w,
		http.StatusOK,
		map[string]string{
			"status": "ok",
		},
	)
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	payload any,
) {

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.Header().Set(
		"Access-Control-Allow-Origin",
		"*",
	)

	w.Header().Set(
		"Cache-Control",
		"no-store",
	)

	w.WriteHeader(status)

	if err := json.NewEncoder(w).
		Encode(payload); err != nil {

		http.Error(
			w,
			"json encoding failure",
			http.StatusInternalServerError,
		)
	}
}

func writeError(
	w http.ResponseWriter,
	status int,
	message string,
) {

	writeJSON(
		w,
		status,
		map[string]string{
			"error": message,
		},
	)
}

func withLogging(
	logger *log.Logger,
	next http.Handler,
) http.Handler {

	return http.HandlerFunc(
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {

			start :=
				time.Now()

			next.ServeHTTP(
				w,
				r,
			)

			logger.Printf(
				"request method=%s path=%s duration=%s remote=%s",
				r.Method,
				r.URL.Path,
				time.Since(start),
				r.RemoteAddr,
			)
		},
	)
}

func withRecovery(
	next http.Handler,
) http.Handler {

	return http.HandlerFunc(
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {

			defer func() {

				if rec := recover(); rec != nil {

					writeError(
						w,
						http.StatusInternalServerError,
						"internal server error",
					)
				}
			}()

			next.ServeHTTP(
				w,
				r,
			)
		},
	)
}