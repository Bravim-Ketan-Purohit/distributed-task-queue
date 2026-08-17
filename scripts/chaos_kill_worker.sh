#!/usr/bin/env bash
# chaos_kill_worker.sh — Kill a worker mid-task, watch reclaim.
#
# Usage: ./scripts/chaos_kill_worker.sh
#
# This script:
# 1. Starts a worker in the background
# 2. Enqueues a long-running task
# 3. Waits for the worker to claim it
# 4. SIGKILL the worker (not SIGTERM — that's graceful, different test)
# 5. Starts a second worker
# 6. Watches the task get reclaimed and completed
#
# Requires: docker-compose services running, venv active

set -euo pipefail

echo "=== Chaos Test: SIGKILL mid-task ==="
echo ""

# Start worker 1
echo "[1] Starting worker-1..."
python -m dtq.worker --queues default --concurrency 1 --worker-id chaos-victim &
WORKER_PID=$!
sleep 2

# Enqueue a task via the API
echo "[2] Enqueuing a slow task..."
curl -s -X POST http://localhost:7201/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "default",
    "task_name": "slow_task",
    "payload": {"sleep_seconds": 30},
    "dedup_key": "chaos-demo-1"
  }' | python -m json.tool

sleep 2

# Kill the worker brutally
echo ""
echo "[3] SIGKILL worker-1 (PID $WORKER_PID)..."
kill -9 $WORKER_PID 2>/dev/null || true
wait $WORKER_PID 2>/dev/null || true
echo "    Worker-1 is dead."

sleep 1

# Start worker 2 — it should reclaim the task
echo ""
echo "[4] Starting worker-2 (should reclaim the orphaned task)..."
python -m dtq.worker --queues default --concurrency 1 --worker-id chaos-rescuer &
WORKER2_PID=$!

echo "    Waiting for reclaim..."
sleep 10

# Check task state
echo ""
echo "[5] Checking task state..."
curl -s http://localhost:7201/v1/tasks/$(curl -s http://localhost:7201/v1/queues | python -c "import sys; print('check logs')") 2>/dev/null || echo "    Check the control plane for reclaim event."

# Cleanup
echo ""
echo "[6] Cleaning up..."
kill $WORKER2_PID 2>/dev/null || true
wait $WORKER2_PID 2>/dev/null || true

echo ""
echo "=== Done. Check logs for reclaim event. ==="
