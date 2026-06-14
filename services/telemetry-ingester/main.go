package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	BrokerAddr = "127.0.0.1:9092"

	Topic = "benchmark-results"

	ConsumerGroup = "telemetry-ingester-group"

	DBFile = "./telemetry.db"

	MaxBatchSize = 500

	FlushInterval = 2 * time.Second

	PollTimeout = 5 * time.Second
)

type TelemetryPayload struct {
	SubmissionID string  `json:"submission_id"`

	P50Micros int64 `json:"p50_micros"`

	P90Micros int64 `json:"p90_micros"`

	P99Micros int64 `json:"p99_micros"`

	TotalOrders int64 `json:"total_orders"`

	SuccessRate float64 `json:"success_rate"`

	Timestamp int64 `json:"timestamp"`
}

type StorageRecord struct {
	Payload TelemetryPayload

	Record *kgo.Record
}

func main() {

	logger := log.New(
		os.Stdout,
		"[telemetry-ingester] ",
		log.LstdFlags|log.Lmicroseconds,
	)

	ctx, cancel :=
		context.WithCancel(
			context.Background(),
		)

	defer cancel()

	db, err := initDB()
	if err != nil {

		logger.Fatalf(
			"database initialization failed: %v",
			err,
		)
	}

	defer db.Close()

	client, err := createKafkaClient()
	if err != nil {

		logger.Fatalf(
			"kafka client initialization failed: %v",
			err,
		)
	}

	defer client.Close()

	logger.Printf(
		"telemetry ingester started broker=%s topic=%s",
		BrokerAddr,
		Topic,
	)

	recordChan :=
		make(chan StorageRecord, 10_000)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {

		defer wg.Done()

		runStorageWorker(
			ctx,
			client,
			db,
			recordChan,
			logger,
		)
	}()

	setupGracefulShutdown(
		cancel,
		logger,
	)

	runConsumerLoop(
		ctx,
		client,
		recordChan,
		logger,
	)

	close(recordChan)

	wg.Wait()

	logger.Println(
		"shutdown complete",
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

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	schema := `
	CREATE TABLE IF NOT EXISTS benchmark_metrics (
		id SERIAL,
		submission_id TEXT NOT NULL,
		p50_micros BIGINT NOT NULL,
		p90_micros BIGINT NOT NULL,
		p99_micros BIGINT NOT NULL,
		total_orders BIGINT NOT NULL,
		success_rate DOUBLE PRECISION NOT NULL,
		timestamp BIGINT NOT NULL,
		PRIMARY KEY (id, timestamp)
	);

	CREATE INDEX IF NOT EXISTS idx_submission_id
	ON benchmark_metrics(submission_id);

	CREATE INDEX IF NOT EXISTS idx_timestamp
	ON benchmark_metrics(timestamp);
	`

	_, err = db.Exec(schema)
	if err != nil {
		return nil, err
	}

	// Make it a TimescaleDB hypertable if not already
	_, _ = db.Exec("SELECT create_hypertable('benchmark_metrics', 'timestamp', chunk_time_interval => 86400000, if_not_exists => TRUE);")

	return db, nil
}

func createKafkaClient() (*kgo.Client, error) {

	return kgo.NewClient(
		kgo.SeedBrokers(
			BrokerAddr,
		),

		kgo.ConsumerGroup(
			ConsumerGroup,
		),

		kgo.ConsumeTopics(
			Topic,
		),

		kgo.DisableAutoCommit(),

		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequestTimeoutOverhead(10 * time.Second),
		kgo.DialTimeout(5 * time.Second),
		kgo.MetadataMaxAge(30 * time.Second),
	)
}

func runConsumerLoop(
	ctx context.Context,
	client *kgo.Client,
	recordChan chan<- StorageRecord,
	logger *log.Logger,
) {

	for {

		select {

		case <-ctx.Done():
			return

		default:
		}

		pollCtx, cancel :=
			context.WithTimeout(
				ctx,
				PollTimeout,
			)

		fetches :=
			client.PollFetches(
				pollCtx,
			)

		cancel()

		if fetches.IsClientClosed() {

			logger.Println(
				"client closed",
			)

			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {

			for _, err := range errs {

				logger.Printf(
					"kafka fetch error topic=%s partition=%d err=%v",
					err.Topic,
					err.Partition,
					err.Err,
				)
			}

			continue
		}

		iter :=
			fetches.RecordIter()

		for !iter.Done() {

			record :=
				iter.Next()

			payload, err :=
				parseTelemetryRecord(
					record.Value,
				)

			if err != nil {

				logger.Printf(
					"invalid telemetry payload err=%v",
					err,
				)

				continue
			}

			select {

			case recordChan <- StorageRecord{
				Payload: payload,
				Record:  record,
			}:

			case <-ctx.Done():
				return
			}
		}
	}
}

func runStorageWorker(
	ctx context.Context,
	client *kgo.Client,
	db *sql.DB,
	recordChan <-chan StorageRecord,
	logger *log.Logger,
) {

	ticker :=
		time.NewTicker(
			FlushInterval,
		)

	defer ticker.Stop()

	buffer :=
		make([]StorageRecord, 0, MaxBatchSize)

	flush := func() {

		if len(buffer) == 0 {
			return
		}

		if err := persistBatch(
			ctx,
			db,
			buffer,
		); err != nil {

			logger.Printf(
				"batch persistence failed err=%v",
				err,
			)

			return
		}

		logger.Printf(
			"persisted telemetry batch size=%d",
			len(buffer),
		)

		if len(buffer) > 0 {
			uncommittedRecords := make([]*kgo.Record, len(buffer))
			for i, r := range buffer {
				uncommittedRecords[i] = r.Record
			}
			if err := client.CommitRecords(ctx, uncommittedRecords...); err != nil {
				logger.Printf("[-] Failed to commit offsets to Redpanda: %v", err)
			}
		}
		buffer = buffer[:0]
	}

	for {

		select {

		case <-ctx.Done():

			flush()

			return

		case record, ok := <-recordChan:

			if !ok {

				flush()

				return
			}

			buffer =
				append(
					buffer,
					record,
				)

			if len(buffer) >= MaxBatchSize {

				flush()
			}

		case <-ticker.C:

			flush()
		}
	}
}

func persistBatch(
	ctx context.Context,
	db *sql.DB,
	records []StorageRecord,
) error {

	tx, err :=
		db.BeginTx(
			ctx,
			nil,
		)

	if err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(
		ctx,
		`
		INSERT INTO benchmark_metrics (
			submission_id,
			p50_micros,
			p90_micros,
			p99_micros,
			total_orders,
			success_rate,
			timestamp
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		`,
	)

	if err != nil {

		_ = tx.Rollback()

		return err
	}

	defer stmt.Close()

	for _, record := range records {

		payload := record.Payload

		_, err := stmt.ExecContext(
			ctx,
			payload.SubmissionID,
			payload.P50Micros,
			payload.P90Micros,
			payload.P99Micros,
			payload.TotalOrders,
			payload.SuccessRate,
			payload.Timestamp,
		)

		if err != nil {

			_ = tx.Rollback()

			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func parseTelemetryRecord(
	raw []byte,
) (TelemetryPayload, error) {

	var payload TelemetryPayload

	if err := json.Unmarshal(
		raw,
		&payload,
	); err != nil {

		return TelemetryPayload{}, err
	}

	if payload.SubmissionID == "" {

		return TelemetryPayload{},
			errors.New(
				"submission_id missing",
			)
	}

	return payload, nil
}

func setupGracefulShutdown(
	cancel context.CancelFunc,
	logger *log.Logger,
) {

	signalChan :=
		make(chan os.Signal, 1)

	signal.Notify(
		signalChan,
		os.Interrupt,
		syscall.SIGTERM,
	)

	go func() {

		sig := <-signalChan

		logger.Printf(
			"shutdown signal received signal=%s",
			sig.String(),
		)

		cancel()
	}()
}