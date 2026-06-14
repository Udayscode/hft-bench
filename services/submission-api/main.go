package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	bench "github.com/uday/hft-bench/services/submission-api/internal/pb/benchmark"
	orch "github.com/uday/hft-bench/services/submission-api/internal/pb/orchestrator"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	ListenAddr = ":8080"

	OrchestratorURL = "localhost:50051"

	BotFleetURL = "[::1]:50052"

	DBFile = "../telemetry-ingester/telemetry.db"

	MaxUploadSize = 32 << 20

	RequestTimeout = 90 * time.Second

	BenchmarkDBPollTimeout = 5 * time.Second

	BenchmarkDBPollInterval = 250 * time.Millisecond
)

type SubmissionResponse struct {
	SubmissionID string  `json:"submission_id"`

	P99Micros int64 `json:"p99_micros"`

	Score float64 `json:"score"`

	SuccessRate float64 `json:"success_rate"`

	TotalOrders int64 `json:"total_orders"`
}

type SubmissionServer struct {
	logger *log.Logger

	db *sql.DB

	orchClient orch.VMControllerClient

	benchClient bench.BenchmarkControllerClient
}

type App struct {
	httpServer *http.Server

	submissionServer *SubmissionServer

	logger *log.Logger
}

func main() {

	logger := log.New(
		os.Stdout,
		"[submission-api] ",
		log.LstdFlags|log.Lmicroseconds,
	)

	db, err := initDB()
	if err != nil {

		logger.Fatalf(
			"database init failed: %v",
			err,
		)
	}

	defer db.Close()

	orchConn, err := grpc.NewClient(
		OrchestratorURL,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {

		logger.Fatalf(
			"failed connecting orchestrator: %v",
			err,
		)
	}

	defer orchConn.Close()

	benchConn, err := grpc.NewClient(
		BotFleetURL,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {

		logger.Fatalf(
			"failed connecting bot fleet: %v",
			err,
		)
	}

	defer benchConn.Close()

	submissionServer := &SubmissionServer{
		logger: logger,

		db: db,

		orchClient:
			orch.NewVMControllerClient(
				orchConn,
			),

		benchClient:
			bench.NewBenchmarkControllerClient(
				benchConn,
			),
	}

	mux := http.NewServeMux()

	mux.HandleFunc(
		"/health",
		healthHandler,
	)

	mux.HandleFunc(
		"/submit",
		submissionServer.handleSubmit,
	)

	mux.HandleFunc(
		"/status",
		submissionServer.handleStatus,
	)

	go submissionServer.startWorkerPool(2)

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
			15 * time.Second,

		WriteTimeout:
			120 * time.Second,

		IdleTimeout:
			60 * time.Second,

		MaxHeaderBytes:
			1 << 20,
	}

	app := &App{
		httpServer: httpServer,

		submissionServer:
			submissionServer,

		logger: logger,
	}

	app.run()
}

func (a *App) run() {

	errChan := make(
		chan error,
		1,
	)

	go func() {

		a.logger.Printf(
			"submission api listening addr=%s",
			ListenAddr,
		)

		listener, err := net.Listen(
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

	shutdownSignal :=
		make(chan os.Signal, 1)

	signal.Notify(
		shutdownSignal,
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

	case sig := <-shutdownSignal:

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
		"submission api shutdown complete",
	)
}

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
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS benchmark_jobs (
    submission_id VARCHAR PRIMARY KEY,
    status VARCHAR NOT NULL DEFAULT 'PENDING',
    score NUMERIC DEFAULT 0,
    p99_micros NUMERIC DEFAULT 0,
    success_rate NUMERIC DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
`)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s *SubmissionServer) handleSubmit(
	w http.ResponseWriter,
	r *http.Request,
) {

	if r.Method != http.MethodPost {

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
			RequestTimeout,
		)

	defer cancel()

	r.Body = http.MaxBytesReader(
		w,
		r.Body,
		MaxUploadSize,
	)

	if err := r.ParseMultipartForm(
		MaxUploadSize,
	); err != nil {

		writeError(
			w,
			http.StatusBadRequest,
			"invalid multipart payload",
		)

		return
	}

	submissionID :=
		strings.TrimSpace(
			r.FormValue(
				"submission_id",
			),
		)

	if submissionID == "" {

		writeError(
			w,
			http.StatusBadRequest,
			"submission_id required",
		)

		return
	}

	file, _, err :=
		r.FormFile("binary")

	if err != nil {

		writeError(
			w,
			http.StatusBadRequest,
			"binary payload missing",
		)

		return
	}

	defer file.Close()

	tempBinaryPath, err :=
		saveUploadedBinary(
			submissionID,
			file,
		)

	if err != nil {

		s.logger.Printf(
			"binary save failure submission_id=%s err=%v",
			submissionID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed storing binary",
		)

		return
	}

	defer os.Remove(
		tempBinaryPath,
	)

	s.logger.Printf(
		"submission received submission_id=%s path=%s",
		submissionID,
		tempBinaryPath,
	)

		_, err = s.db.ExecContext(ctx, "INSERT INTO benchmark_jobs (submission_id, status) VALUES ($1, 'PENDING') ON CONFLICT (submission_id) DO UPDATE SET status = 'PENDING', score = 0, p99_micros = 0, success_rate = 0, updated_at = CURRENT_TIMESTAMP", submissionID)
	if err != nil {
		s.logger.Printf("failed to insert job err=%v", err)
		writeError(w, http.StatusInternalServerError, "failed to queue job")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"submission_id": submissionID,
		"status":        "PENDING",
		"message":       "Benchmark job queued successfully",
	})
}

func (s *SubmissionServer) pollBenchmarkMetrics(
	ctx context.Context,
	submissionID string,
	minID int64,
) (int64, int64, float64, error) {

	timeout :=
		time.After(
			BenchmarkDBPollTimeout,
		)

	ticker :=
		time.NewTicker(
			BenchmarkDBPollInterval,
		)

	defer ticker.Stop()

	for {

		select {

		case <-ctx.Done():

			return 0,
				0,
				0,
				ctx.Err()

		case <-timeout:

			return 0,
				0,
				0,
				errors.New(
					"benchmark polling timeout",
				)

		case <-ticker.C:

			var p50 int64
			var p99 int64

			var successRate float64

			err := s.db.QueryRowContext(
				ctx,
				`
				SELECT
					p50_micros,
					p99_micros,
					success_rate
				FROM benchmark_metrics
				WHERE submission_id = $1
				  AND id > $2
				ORDER BY id DESC
				LIMIT 1
				`,
				submissionID,
				minID,
			).Scan(
				&p50,
				&p99,
				&successRate,
			)

			if err == nil {

				return p50,
					p99,
					successRate,
					nil
			}
		}
	}
}

func saveUploadedBinary(
	submissionID string,
	file io.Reader,
) (
	string,
	error,
) {

	tempPath :=
		filepath.Join(
			os.TempDir(),
			fmt.Sprintf(
				"submission-%s.bin",
				submissionID,
			),
		)

	output, err :=
		os.OpenFile(
			tempPath,
			os.O_CREATE|
				os.O_TRUNC|
				os.O_WRONLY,
			0755,
		)

	if err != nil {

		return "",
			err
	}

	defer output.Close()

	if _, err := io.Copy(
		output,
		file,
	); err != nil {

		return "",
			err
	}

	return tempPath,
		nil
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

	return math.Round(score*100) / 100
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
				"request method=%s path=%s duration=%s",
				r.Method,
				r.URL.Path,
				time.Since(start),
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
func (s *SubmissionServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	submissionID := r.URL.Query().Get("submission_id")
	if submissionID == "" {
		writeError(w, http.StatusBadRequest, "submission_id required")
		return
	}

	var status string
	var score, p99, success float64
	err := s.db.QueryRow("SELECT status, score, p99_micros, success_rate FROM benchmark_jobs WHERE submission_id = $1", submissionID).Scan(&status, &score, &p99, &success)
	if err != nil {
		writeError(w, http.StatusNotFound, "submission not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"submission_id": submissionID,
		"status":        status,
		"score":         score,
		"p99_micros":    p99,
		"success_rate":  success,
	})
}

func (s *SubmissionServer) startWorkerPool(concurrency int) {
	s.logger.Printf("Starting async benchmark worker pool with concurrency=%d", concurrency)
	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			for {
				time.Sleep(1 * time.Second)
				s.processNextJob(workerID)
			}
		}(i)
	}
}

func (s *SubmissionServer) processNextJob(workerID int) {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()

	var submissionID string
	err = tx.QueryRow(`
		SELECT submission_id FROM benchmark_jobs 
		WHERE status = 'PENDING' 
		FOR UPDATE SKIP LOCKED LIMIT 1
	`).Scan(&submissionID)

	if err != nil {
		return // No pending jobs
	}

	_, err = tx.Exec("UPDATE benchmark_jobs SET status = 'RUNNING', updated_at = CURRENT_TIMESTAMP WHERE submission_id = $1", submissionID)
	if err != nil {
		return
	}
	tx.Commit()

	s.logger.Printf("[Worker %d] Picked up job submission_id=%s", workerID, submissionID)
	s.runBenchmarkSync(ctx, submissionID)
}

func (s *SubmissionServer) runBenchmarkSync(ctx context.Context, submissionID string) {
	spawnResponse, err := s.orchClient.SpawnVM(ctx, &orch.SpawnRequest{
		SubmissionId: submissionID,
		VcpuCount:    1,
		MemoryMib:    256,
	})

	if err != nil || !spawnResponse.Success {
		s.logger.Printf("vm spawn failed submission_id=%s err=%v", submissionID, err)
		s.db.Exec("UPDATE benchmark_jobs SET status = 'FAILED', updated_at = CURRENT_TIMESTAMP WHERE submission_id = $1", submissionID)
		return
	}

	vmID := spawnResponse.VmId
	targetIP := spawnResponse.IpAddress
	if targetIP == "" {
		targetIP = "172.16.0.2"
	}

	defer func() {
		s.orchClient.TeardownVM(context.Background(), &orch.TeardownRequest{VmId: vmID})
	}()

	time.Sleep(100 * time.Millisecond)

	benchResponse, err := s.benchClient.StartBenchmark(ctx, &bench.BenchmarkRequest{
		VmId:        submissionID,
		TargetIp:    targetIP,
		Port:        8080,
		TotalOrders: 1000,
		Concurrency: 1,
		Mode:        "burst",
		VsockPath:   spawnResponse.VsockPath,
	})

	if err != nil || !benchResponse.Success {
		s.logger.Printf("benchmark failed submission_id=%s err=%v", submissionID, err)
		s.db.Exec("UPDATE benchmark_jobs SET status = 'FAILED', updated_at = CURRENT_TIMESTAMP WHERE submission_id = $1", submissionID)
		return
	}

	var minID int64
	_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id), 0) FROM benchmark_metrics`).Scan(&minID)

	p50Micros, p99Micros, successRate, err := s.pollBenchmarkMetrics(ctx, submissionID, minID)
	if err != nil {
		s.logger.Printf("db poll failed submission_id=%s err=%v", submissionID, err)
		if benchResponse.Latencies != nil {
			p50Micros = int64(benchResponse.Latencies.P50Micros)
			p99Micros = int64(benchResponse.Latencies.P99Micros)
		}
		totalAcks := int64(benchResponse.SuccessfulAcks) + int64(benchResponse.FailedOrders)
		if totalAcks > 0 {
			successRate = float64(benchResponse.SuccessfulAcks) / float64(totalAcks)
		} else {
			successRate = 1.0
		}
	}

	score := calculateScore(p50Micros, p99Micros, successRate)

	_, err = s.db.Exec(`
		UPDATE benchmark_jobs 
		SET status = 'COMPLETED', score = $1, p99_micros = $2, success_rate = $3, updated_at = CURRENT_TIMESTAMP 
		WHERE submission_id = $4
	`, score, p99Micros, successRate, submissionID)
	
	if err != nil {
		s.logger.Printf("failed to update job status submission_id=%s err=%v", submissionID, err)
	} else {
		s.logger.Printf("job completed submission_id=%s score=%.2f", submissionID, score)
	}
}
