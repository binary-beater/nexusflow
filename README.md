# NexusFlow

Deterministic, failure-atomic workflow orchestration engine backed by PostgreSQL.

## Phase 1 Quick Start (Definition Ingestion & Persistence)

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.12+ and `uv`

### 2. Configure Environment
Copy `.env.example` to `.env` and set local secrets:
```bash
cp .env.example .env
```

### 3. Start PostgreSQL 16
```bash
docker compose up -d postgres
```

### 4. Run Database Migrations
```bash
uv run alembic upgrade head
```

### 5. Start NexusFlow Control Plane
```bash
uv run uvicorn nexusflow.interfaces.http.app:app --host 0.0.0.0 --port 8000 --workers 1
```

### 6. Register a Workflow Definition
```bash
curl -X POST http://localhost:8000/v1/definitions \
  -H "Content-Type: application/yaml" \
  -H "Authorization: Bearer <PUBLIC_CLIENT_SECRET>" \
  -H "Idempotency-Key: demo-key-1" \
  --data-binary "@examples/workflows/01_linear_success.yaml"
```

### 7. Run Verification Test Suites
```bash
# Unit & PostgreSQL integration test suite
uv run pytest

# Static type check & linter
uv run pyright
uv run ruff check .
```
