#!/usr/bin/env bash
# quickstart.sh — Set up and run the full DTQ stack locally.
#
# Prerequisites: uv, Docker, Node 22
#
# Usage: ./scripts/quickstart.sh

set -euo pipefail

echo "=== DTQ Quickstart ==="

# 1. Python environment
echo "[1/6] Setting up Python environment..."
if ! command -v uv &> /dev/null; then
    echo "ERROR: uv not found. Install it: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
fi
uv venv --python 3.12
source .venv/bin/activate
uv pip install -e ".[dev]"

# 2. Docker services
echo "[2/6] Starting Docker services (Redis + Postgres)..."
docker compose -f docker-compose.dev.yml up -d redis postgres
echo "    Waiting for services..."
sleep 3

# 3. Database migration
echo "[3/6] Running database migrations..."
alembic upgrade head

# 4. Web console
echo "[4/6] Installing web console dependencies..."
cd web && npm install && cd ..

# 5. Verify
echo "[5/6] Running unit tests..."
pytest tests/unit -q

echo "[6/6] Running property tests..."
pytest tests/property -q

echo ""
echo "=== Ready! ==="
echo ""
echo "Start the control plane:  uvicorn dtq.control.app:app --reload --port 7201"
echo "Start a worker:           python -m dtq.worker --queues default --concurrency 8"
echo "Start the console:        cd web && npm run dev"
echo ""
echo "For Kafka/RabbitMQ:       docker compose -f docker-compose.dev.yml --profile brokers up -d"
echo "For observability:        docker compose -f docker-compose.dev.yml --profile observability up -d"
echo ""
echo "Build the C++ loadgen:    cmake -S bench -B bench/build -DCMAKE_BUILD_TYPE=Release && cmake --build bench/build --parallel 10"
