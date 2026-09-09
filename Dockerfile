FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
RUN pip install --upgrade pip && pip install uv && uv pip install --system .

COPY src/ src/
COPY migrations/ migrations/
COPY alembic.ini .

EXPOSE 8000

CMD ["uvicorn", "nexusflow.interfaces.http.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
