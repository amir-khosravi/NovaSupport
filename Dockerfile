# ── Stage 1: install Python dependencies ──────────────────────────────────────
FROM python:3.12-slim AS deps

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
# Install all declared dependencies into a target directory
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --target /deps \
        "langgraph>=0.2.0" \
        "langchain-core>=0.3.0" \
        "langchain-anthropic>=0.3.0" \
        "langchain-community>=0.3.0" \
        "pydantic>=2.7.0" \
        "pydantic-settings>=2.3.0" \
        "fastapi>=0.115.0" \
        "uvicorn[standard]>=0.30.0" \
        "sqlalchemy[asyncio]>=2.0.0" \
        "alembic>=1.13.0" \
        "asyncpg>=0.29.0" \
        "aiokafka>=0.11.0" \
        "qdrant-client>=1.9.0" \
        "redis[hiredis]>=5.0.0" \
        "httpx>=0.27.0" \
        "structlog>=24.2.0" \
        "opentelemetry-sdk>=1.25.0" \
        "opentelemetry-instrumentation-fastapi>=0.46b0" \
        "prometheus-client>=0.20.0" \
        "sentence-transformers>=3.0.0" \
        "python-frontmatter>=1.1.0" \
        "tiktoken>=0.7.0" \
        "python-ulid>=2.2.0" \
        "tenacity>=8.3.0" \
        "mcp[cli]>=1.27.2"

# ── Stage 2: lean runtime image ────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Runtime system deps (asyncpg needs libpq at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from deps stage
COPY --from=deps /deps /usr/local/lib/python3.12/site-packages

# Copy application source
COPY . .

# Non-root user
RUN useradd -m -u 1001 novasupport \
    && chown -R novasupport:novasupport /app
USER novasupport

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# Default: run the API (override `command` in docker-compose for other services)
CMD ["uvicorn", "apps.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
