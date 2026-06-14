#!/bin/bash
echo "🚀 INITIATING CONCURRENT MICRO-VM BLAST LOOP (CONCURRENCY=10)..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_PATH="$SCRIPT_DIR/../sandbox/strategy_bin"

# Parallel multi-processing curl spawns
curl -s -X POST http://localhost:8080/submit -F "submission_id=sc--11" -F "binary=@$BINARY_PATH" &
curl -s -X POST http://localhost:8080/submit -F "submission_id=sc--12" -F "binary=@$BINARY_PATH" &
curl -s -X POST http://localhost:8080/submit -F "submission_id=sc--13" -F "binary=@$BINARY_PATH" &
curl -s -X POST http://localhost:8080/submit -F "submission_id=sc--14" -F "binary=@$BINARY_PATH" &
curl -s -X POST http://localhost:8080/submit -F "submission_id=sc--15" -F "binary=@$BINARY_PATH" &
curl -s -X POST http://localhost:8080/submit -F "submission_id=sc--16" -F "binary=@$BINARY_PATH" &
curl -s -X POST http://localhost:8080/submit -F "submission_id=sc--17" -F "binary=@$BINARY_PATH" &
curl -s -X POST http://localhost:8080/submit -F "submission_id=sc--18" -F "binary=@$BINARY_PATH" &
curl -s -X POST http://localhost:8080/submit -F "submission_id=sc--19" -F "binary=@$BINARY_PATH" &
curl -s -X POST http://localhost:8080/submit -F "submission_id=sc--20" -F "binary=@$BINARY_PATH" &

# Wait for all background parallel curls to finish execution
wait
echo "🏁 BLAST MATRIX COMPLETED. CHECKING LEADERBOARD INTEGRITY..."
