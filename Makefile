# ============================================================================
# Distributed Job Processing System — Makefile
# ============================================================================

ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# ── Go ───────────────────────────────────────────────────────────────────────
BINARY_NAME := server
BUILD_DIR   := $(ROOT)bin
CMD_PATH    := ./cmd/api

# ── Docker ───────────────────────────────────────────────────────────────────
COMPOSE     := docker compose
APP_NAME    := distributed-job-processor
AUTH_HEADERS := -H "X-Client-ID: service-a" -H "X-API-Key: key-a"

.PHONY: all build run clean \
        up down restart logs \
        tidy fmt vet lint \
        test test-race test-cover \
        smoke smoke-phase4 \
        redis-cli psql \
        help

# ============================================================================
# Default
# ============================================================================

all: tidy fmt vet build

# ============================================================================
# Build
# ============================================================================

build:
	@echo "→ Building binary..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_PATH)
	@echo "✓ Binary at $(BUILD_DIR)/$(BINARY_NAME)"

run: build
	@echo "→ Running locally (requires Postgres + Redis running)..."
	$(BUILD_DIR)/$(BINARY_NAME)

clean:
	@echo "→ Cleaning build artifacts..."
	@rm -rf $(BUILD_DIR)
	@echo "✓ Done"

# ============================================================================
# Docker Compose
# ============================================================================

up:
	@echo "→ Starting all services..."
	$(COMPOSE) -f $(ROOT)docker-compose.yml up --build -d
	@echo ""
	@echo "✓ All services started"
	@echo ""
	@echo "  API          → http://localhost:8085"
	@echo ""
	@echo "  Run 'make smoke' to send test jobs"

down:
	@echo "→ Stopping all services..."
	$(COMPOSE) -f $(ROOT)docker-compose.yml down
	@echo "✓ Done"

down-v:
	@echo "→ Stopping all services and removing volumes..."
	$(COMPOSE) -f $(ROOT)docker-compose.yml down -v
	@echo "✓ Done (volumes removed)"

restart:
	@echo "→ Restarting app container..."
	$(COMPOSE) -f $(ROOT)docker-compose.yml restart app

logs:
	$(COMPOSE) -f $(ROOT)docker-compose.yml logs -f app

logs-all:
	$(COMPOSE) -f $(ROOT)docker-compose.yml logs -f

ps:
	$(COMPOSE) -f $(ROOT)docker-compose.yml ps

# ============================================================================
# Go tooling
# ============================================================================

tidy:
	@echo "→ Tidying modules..."
	go mod tidy

fmt:
	@echo "→ Formatting..."
	go fmt ./...

vet:
	@echo "→ Vetting..."
	go vet ./...

lint:
	@echo "→ Linting (requires golangci-lint)..."
	golangci-lint run ./...

# ============================================================================
# Tests
# ============================================================================

test:
	@echo "→ Running tests..."
	go test ./... -count=1

test-race:
	@echo "→ Running tests with race detector..."
	go test ./... -race -count=1

test-cover:
	@echo "→ Running tests with coverage..."
	go test ./... -race -count=1 -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo "✓ Coverage report → coverage.html"

# ============================================================================
# Smoke tests — run after make up
# ============================================================================

smoke:
	@echo ""
	@echo "════════════════════════════════════════"
	@echo " Smoke Test — Phase 3 (core job flow)"
	@echo "════════════════════════════════════════"

	@echo "\n── Health checks ──"
	@curl -sf http://localhost:8085/health/live   && echo " /health/live  → ok" || echo " /health/live  → FAILED"
	@curl -sf http://localhost:8085/health/ready  && echo " /health/ready → ok" || echo " /health/ready → FAILED"

	@echo "\n── Submit noop job ──"
	@curl -s -X POST http://localhost:8085/api/v1/jobs \
	  $(AUTH_HEADERS) \
	  -H "Content-Type: application/json" \
	  -d '{"job_type":"noop","queue":"default","payload":{}}' | jq .

	@echo "\n── Submit send_email job ──"
	@curl -s -X POST http://localhost:8085/api/v1/jobs \
	  $(AUTH_HEADERS) \
	  -H "Content-Type: application/json" \
	  -d '{"job_type":"send_email","queue":"default","payload":{"to":"test@example.com","template":"welcome"}}' | jq .

	@echo "\n── Submit fail_always job (will retry then DLQ) ──"
	@curl -s -X POST http://localhost:8085/api/v1/jobs \
	  $(AUTH_HEADERS) \
	  -H "Content-Type: application/json" \
	  -d '{"job_type":"fail_always","queue":"default","payload":{},"max_attempts":1}' | jq .

	@echo "\n── List jobs ──"
	@curl -s $(AUTH_HEADERS) "http://localhost:8085/api/v1/jobs?limit=5" | jq .

	@echo "\n── Queue stats (default) ──"
	@curl -s $(AUTH_HEADERS) http://localhost:8085/api/v1/queues/default/stats | jq .

	@echo "\n── DLQ (wait ~5s for fail_always to exhaust) ──"
	@sleep 5
	@curl -s $(AUTH_HEADERS) "http://localhost:8085/api/v1/dlq?queue=default" | jq .


smoke-phase4:
	@echo ""
	@echo "════════════════════════════════════════"
	@echo " Smoke Test — Phase 4 (Redis + priority)"
	@echo "════════════════════════════════════════"

	@echo "\n── Submit high-priority job (critical queue, priority=1) ──"
	@curl -s -X POST http://localhost:8085/api/v1/jobs \
	  $(AUTH_HEADERS) \
	  -H "Content-Type: application/json" \
	  -d '{"job_type":"noop","queue":"critical","priority":1,"payload":{}}' | jq .

	@echo "\n── Submit low-priority job (batch queue, priority=9) ──"
	@curl -s -X POST http://localhost:8085/api/v1/jobs \
	  $(AUTH_HEADERS) \
	  -H "Content-Type: application/json" \
	  -d '{"job_type":"noop","queue":"batch","priority":9,"payload":{}}' | jq .

	@echo "\n── Submit scheduled job (15s from now) ──"
	@curl -s -X POST http://localhost:8085/api/v1/jobs \
	  $(AUTH_HEADERS) \
	  -H "Content-Type: application/json" \
	  -d '{"job_type":"noop","queue":"default","priority":5,"payload":{},"run_at":"$(shell date -u -v+15S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '+15 seconds' +%Y-%m-%dT%H:%M:%SZ)"}' | jq .

	@echo "\n── Queue stats (all queues) ──"
	@for q in critical default batch; do \
		echo "  $$q:"; \
		curl -s $(AUTH_HEADERS) http://localhost:8085/api/v1/queues/$$q/stats | jq .; \
	done

	@echo "\n── Redis queue depth (via metrics) ──"
	@curl -s http://localhost:8085/metrics | grep -E "^redis_queue" || true

	@echo "\n── Waiting 20s for scheduled job to promote and run... ──"
	@sleep 20
	@curl -s $(AUTH_HEADERS) "http://localhost:8085/api/v1/jobs?queue=default&status=completed&limit=5" | jq .

# ============================================================================
# DLQ helpers
# ============================================================================

dlq-list:
	@curl -s $(AUTH_HEADERS) "http://localhost:8085/api/v1/dlq?queue=$(QUEUE)&limit=20" | jq .

dlq-stats:
	@curl -s $(AUTH_HEADERS) "http://localhost:8085/api/v1/dlq/stats?queue=$(QUEUE)" | jq .

dlq-replay:
	@echo "→ Replaying DLQ entry $(ID)..."
	@curl -s $(AUTH_HEADERS) -X POST http://localhost:8085/api/v1/dlq/$(ID)/replay | jq .

dlq-bulk-replay:
	@echo "→ Bulk replaying queue $(QUEUE)..."
	@curl -s $(AUTH_HEADERS) -X POST "http://localhost:8085/api/v1/dlq/bulk-replay?queue=$(QUEUE)" | jq .

dlq-purge:
	@echo "→ Purging DLQ entries older than $(DAYS) days..."
	@curl -s $(AUTH_HEADERS) -X POST http://localhost:8085/api/v1/dlq/purge \
	  -H "Content-Type: application/json" \
	  -d '{"older_than_days":$(DAYS),"queue":"$(QUEUE)"}' | jq .

# Usage:
#   make dlq-list QUEUE=default
#   make dlq-stats QUEUE=default
#   make dlq-replay ID=<dlq-entry-uuid>
#   make dlq-bulk-replay QUEUE=default
#   make dlq-purge QUEUE=default DAYS=30

# ============================================================================
# Database helpers
# ============================================================================

psql:
	@echo "→ Connecting to Postgres..."
	$(COMPOSE) -f $(ROOT)docker-compose.yml exec postgres \
	  psql -U app -d jobprocessor

db-jobs:
	@echo "→ Recent jobs..."
	$(COMPOSE) -f $(ROOT)docker-compose.yml exec postgres \
	  psql -U app -d jobprocessor -c \
	  "SELECT id, job_type, queue, status, attempt_count, created_at FROM jobs ORDER BY created_at DESC LIMIT 20;"

db-dlq:
	@echo "→ Dead letter queue..."
	$(COMPOSE) -f $(ROOT)docker-compose.yml exec postgres \
	  psql -U app -d jobprocessor -c \
	  "SELECT id, job_type, queue, last_error, died_at, replayed_at FROM dead_letter_jobs ORDER BY died_at DESC LIMIT 20;"

db-attempts:
	@echo "→ Recent job attempts..."
	$(COMPOSE) -f $(ROOT)docker-compose.yml exec postgres \
	  psql -U app -d jobprocessor -c \
	  "SELECT id, job_id, attempt_num, status, error, started_at FROM job_attempts ORDER BY started_at DESC LIMIT 20;"

db-stats:
	@echo "→ Job counts by status..."
	$(COMPOSE) -f $(ROOT)docker-compose.yml exec postgres \
	  psql -U app -d jobprocessor -c \
	  "SELECT queue, status, COUNT(*) FROM jobs GROUP BY queue, status ORDER BY queue, status;"

# ============================================================================
# Redis helpers
# ============================================================================

redis-cli:
	@echo "→ Connecting to Redis..."
	$(COMPOSE) -f $(ROOT)docker-compose.yml exec redis redis-cli

redis-queues:
	@echo "→ Redis queue depths..."
	@$(COMPOSE) -f $(ROOT)docker-compose.yml exec redis redis-cli \
	  eval " \
	    local queues = {'critical','default','batch'} \
	    local out = {} \
	    for _, q in ipairs(queues) do \
	      local a = redis.call('ZCARD', 'queue:'..q) \
	      local s = redis.call('ZCARD', 'scheduled:'..q) \
	      local i = redis.call('ZCARD', 'inflight:'..q) \
	      table.insert(out, q..': active='..a..' scheduled='..s..' inflight='..i) \
	    end \
	    return out \
	  " 0

redis-flush:
	@echo "→ Flushing all Redis keys (dev only!)..."
	$(COMPOSE) -f $(ROOT)docker-compose.yml exec redis redis-cli FLUSHALL
	@echo "✓ Redis flushed"


# ============================================================================
# Load test (requires k6)
# ============================================================================

load-test:
	@echo "→ Running k6 load test..."
	@which k6 > /dev/null || (echo "k6 not found. Install: https://k6.io/docs/get-started/installation/" && exit 1)
	k6 run $(ROOT)deployments/k6_load_test.js

# ============================================================================
# Help
# ============================================================================

help:
	@echo ""
	@echo "Distributed Job Processing System"
	@echo ""
	@echo "Build & Run"
	@echo "  make build          Build the binary"
	@echo "  make run            Build and run locally"
	@echo "  make clean          Remove build artifacts"
	@echo ""
	@echo "Docker"
	@echo "  make up             Start core services (Postgres, Redis, app)"
	@echo "  make down           Stop all services"
	@echo "  make down-v         Stop all services and delete volumes"
	@echo "  make restart        Restart app container"
	@echo "  make logs           Tail app logs"
	@echo "  make logs-all       Tail all service logs"
	@echo "  make ps             Show container status"
	@echo ""
	@echo "Tests"
	@echo "  make test           Run all tests"
	@echo "  make test-race      Run tests with race detector"
	@echo "  make test-cover     Run tests with coverage report"
	@echo ""
	@echo "Smoke Tests"
	@echo "  make smoke          Core job flow (Phase 3)"
	@echo "  make smoke-phase4   Redis + priority + scheduling (Phase 4)"
	@echo ""
	@echo "DLQ"
	@echo "  make dlq-list       QUEUE=default"
	@echo "  make dlq-stats      QUEUE=default"
	@echo "  make dlq-replay     ID=<uuid>"
	@echo "  make dlq-bulk-replay QUEUE=default"
	@echo "  make dlq-purge      QUEUE=default DAYS=30"
	@echo ""
	@echo "Database"
	@echo "  make psql           Open psql shell"
	@echo "  make db-jobs        Recent jobs"
	@echo "  make db-dlq         Dead letter queue"
	@echo "  make db-attempts    Job attempt history"
	@echo "  make db-stats       Job counts by queue + status"
	@echo ""
	@echo "Redis"
	@echo "  make redis-cli      Open redis-cli shell"
	@echo "  make redis-queues   Show queue depths"
	@echo "  make redis-flush    Flush all keys (dev only)"
	@echo ""
	@echo "Load Test"
	@echo "  make load-test      Run k6 load test"
	@echo ""