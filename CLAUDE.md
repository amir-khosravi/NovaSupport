# NovaSupport — Event-Driven Multi-Agent E-Commerce Support Platform

## Project Overview
Event-driven agentic AI platform. Each support ticket flows through Kafka topics; agents (triage, resolver, responder) communicate only via messages — never directly.

## Quick Start (Development Mode)

### Step 1 — Start Infrastructure (Docker)
Run once; containers persist across reboots.
```powershell
docker compose up -d redpanda postgres redis qdrant
```
Or if already running, just verify:
```powershell
docker ps
```
Expected containers: `novasupport-kafka`, `novasupport-postgres`, `novasupport-redis`, `novasupport-qdrant`

### Step 2 — Seed & Index (first time or after reset)
```powershell
python scripts/seed_database.py        # PostgreSQL: customers, orders, payments, shipments
python infrastructure/kafka/create_topics.py   # Kafka: 7 topics
python -m rag.indexer                  # Qdrant: 73 policy chunks
```

### Step 3 — Start Kafka UI (optional, visual inspection)
```powershell
docker run -d --name novasupport-kafka-ui --network novasupport_novasupport -p 8080:8080 -e KAFKA_BROKERS=redpanda:9092 redpandadata/console:v2.5.2
```
Then open http://localhost:8080

### Step 4 — Start Application Services (4 separate terminals)

**Terminal 1 — MCP Server** (operational data from PostgreSQL)
```powershell
python -m mcp_servers.operational_data.server
```
Runs on http://localhost:8001

**Terminal 2 — API + UI**
```powershell
uvicorn apps.api.main:app --reload
```
Runs on http://localhost:8000 — open this in the browser

**Terminal 3 — Orchestrator** (LangGraph workflow coordinator)
```powershell
python -m apps.orchestrator
```

**Terminal 4 — Worker** (task executors: triage, resolve, respond)
```powershell
python -m apps.worker --id worker-1 --count 3
```

## Service Map

| Service       | Port  | Purpose                                      |
|---------------|-------|----------------------------------------------|
| UI / API      | 8000  | Browser interface + REST endpoints           |
| MCP Server    | 8001  | PostgreSQL queries via MCP protocol          |
| Kafka (ext)   | 9094  | Event bus — all agent communication          |
| Kafka UI      | 8080  | Visual topic/message inspector               |
| PostgreSQL    | 5432  | Customer, order, payment, shipment data      |
| Redis         | 6379  | SSE pub/sub + worker idempotency             |
| Qdrant        | 6333  | Vector search for policy RAG                 |

## Kafka Topics

| Topic                  | Producer        | Consumer        | Purpose                        |
|------------------------|-----------------|-----------------|--------------------------------|
| `ticket.created`       | API             | Orchestrator    | New support ticket event       |
| `agent.commands`       | Orchestrator    | Workers         | Task dispatch to agents        |
| `agent.results`        | Workers         | Orchestrator    | Task completion results        |
| `ticket.completed`     | Orchestrator    | —               | Resolved ticket events         |
| `ticket.escalated`     | Orchestrator    | —               | Escalated ticket events        |
| `agent.commands.retry` | Worker          | Workers         | Retry queue                    |
| `agent.commands.dlq`   | Worker          | —               | Dead letter queue              |

## Ticket Pipeline (per ticket)
```
Browser → API → ticket.created → Orchestrator (LangGraph)
  └─ triage_ticket    → agent.commands → Worker → agent.results
  └─ get_order        ┐
  └─ get_shipment     ├─ parallel (MCP → PostgreSQL)
  └─ get_customer     ┘
  └─ retrieve_policy  → Qdrant vector search (real policy docs)
  └─ resolve_ticket   → Claude LLM → action + confidence
  └─ generate_response→ Claude LLM → customer-facing text
  └─ quality_check    → confidence >= 0.75 → approved / escalate
  └─ SSE events → Redis pub/sub → Browser (real-time steps)
```

## Key Files

| File                              | Purpose                                      |
|-----------------------------------|----------------------------------------------|
| `agents/triage.py`                | LLM triage: category + priority              |
| `agents/resolver.py`              | LLM resolver: action + confidence (6 types) |
| `agents/responder.py`             | LLM response generator                       |
| `apps/orchestrator/graph.py`      | LangGraph workflow definition                |
| `apps/worker/handlers.py`         | Task handlers + fallbacks                    |
| `apps/api/routers/stream.py`      | SSE streaming (Redis pub/sub)                |
| `rag/retriever.py`                | Qdrant search via httpx (bypasses client v1.18 bug) |
| `mcp_servers/operational_data/`   | MCP server for PostgreSQL queries            |

## Notes
- RAG retriever uses direct httpx calls to `/points/search` (not qdrant-client) — qdrant-client v1.18 maps search() to `/points/query` which is unsupported on server v1.9.4
- Worker fallback: if LLM unavailable, rule-based logic runs with per-category confidence
- Escalation only triggers when: required fields missing OR confidence < 0.75
- MCP data is real PostgreSQL (Alice Smith, VIP, express, 26-day delay, $24.99 fee)
