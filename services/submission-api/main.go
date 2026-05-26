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

	_ "github.com/mattn/go-sqlite3"

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
		5 * time.Minute,
	)

	if err := db.Ping(); err != nil {

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

	spawnResponse, err :=
		s.orchClient.SpawnVM(ctx,
			&orch.SpawnRequest{
				SubmissionId: submissionID,
				VcpuCount:    1,
				MemoryMib:    256,
			},
		)

	if err != nil {

		writeError(
			w,
			http.StatusInternalServerError,
			fmt.Sprintf(
				"vm spawn failed: %v",
				err,
			),
		)

		return
	}

	if !spawnResponse.Success {

		writeError(
			w,
			http.StatusInternalServerError,
			spawnResponse.ErrorMessage,
		)

		return
	}

	vmID := spawnResponse.VmId

	targetIP :=
		spawnResponse.IpAddress

	if targetIP == "" {

		targetIP = "172.16.0.2"
	}

	s.logger.Printf(
		"vm spawned vm_id=%s ip=%s",
		vmID,
		targetIP,
	)

	defer func() {

		_, err :=
			s.orchClient.TeardownVM(
				context.Background(),
				&orch.TeardownRequest{
					VmId: vmID,
				},
			)

		if err != nil {

			s.logger.Printf(
				"vm teardown failed vm_id=%s err=%v",
				vmID,
				err,
			)
		}
	}()

	time.Sleep(
		3000 * time.Millisecond,
	)

	benchResponse, err :=
		s.benchClient.StartBenchmark(
			ctx,
			&bench.BenchmarkRequest{
				VmId:
					submissionID,

				TargetIp:
					targetIP,

				Port: 8080,

				TotalOrders:
					1000,

				Concurrency:
					1,

				Mode:
					"burst",
			},
		)

	if err != nil {

		writeError(
			w,
			http.StatusInternalServerError,
			fmt.Sprintf(
				"benchmark failed: %v",
				err,
			),
		)

		return
	}

	if !benchResponse.Success {

		writeError(
			w,
			http.StatusInternalServerError,
			benchResponse.ErrorMessage,
		)

		return
	}

	var minID int64
	_ = s.db.QueryRowContext(
		ctx,
		`SELECT COALESCE(MAX(id), 0) FROM benchmark_metrics`,
	).Scan(&minID)

	p99Micros,
		successRate,
		err := s.pollBenchmarkMetrics(
		ctx,
		submissionID,
		minID,
	)

	if err != nil {

		s.logger.Printf(
			"database polling failed fallbacking to grpc metrics submission_id=%s err=%v",
			submissionID,
			err,
		)

		if benchResponse.Latencies != nil {

			p99Micros =
				int64(benchResponse.
					Latencies.
					P99Micros)
		}

		successRate = 1.0
	}

	score :=
		calculateScore(
			p99Micros,
			successRate,
		)

	response :=
		SubmissionResponse{
			SubmissionID:
				submissionID,

			P99Micros:
				p99Micros,

			Score:
				score,

			SuccessRate:
				successRate,

			TotalOrders:
				int64(
					benchResponse.
						SuccessfulAcks +
						benchResponse.
							FailedOrders,
				),
		}

	writeJSON(
		w,
		http.StatusOK,
		response,
	)

	s.logger.Printf(
		"submission complete submission_id=%s p99=%d score=%.2f",
		submissionID,
		p99Micros,
		score,
	)
}

func (s *SubmissionServer) pollBenchmarkMetrics(
	ctx context.Context,
	submissionID string,
	minID int64,
) (
	int64,
	float64,
	error,
) {

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
				ctx.Err()

		case <-timeout:

			return 0,
				0,
				errors.New(
					"benchmark polling timeout",
				)

		case <-ticker.C:

			var p99 int64

			var successRate float64

			err := s.db.QueryRowContext(
				ctx,
				`
				SELECT
					p99_micros,
					success_rate
				FROM benchmark_metrics
				WHERE submission_id = ?
				  AND id > ?
				ORDER BY id DESC
				LIMIT 1
				`,
				submissionID,
				minID,
			).Scan(
				&p99,
				&successRate,
			)

			if err == nil {

				return p99,
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
	p99Micros int64,
	successRate float64,
) float64 {

	if p99Micros <= 0 {
		return 0
	}

	score :=
		(100000.0 /
			float64(
				p99Micros,
			)) *
			successRate *
			100.0

	score =
		math.Min(
			score,
			100.0,
		)

	return math.Round(
		score*100,
	) / 100
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