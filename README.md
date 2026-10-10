<div align="center">

<img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=700&size=32&pause=1000&color=6366F1&center=true&vCenter=true&width=700&lines=NovaSupport;Event-Driven+Multi-Agent+AI;MCP+%2B+RAG+%2B+Kafka+%2B+Claude" alt="Typing SVG" />

<br/>

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Apache Kafka](https://img.shields.io/badge/Kafka-Redpanda-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://redpanda.com)
[![LangGraph](https://img.shields.io/badge/LangGraph-Orchestration-1C3C3C?style=for-the-badge&logo=langchain&logoColor=white)](https://langchain-ai.github.io/langgraph)
[![Claude](https://img.shields.io/badge/Claude-Sonnet_4.6-D97706?style=for-the-badge&logo=anthropic&logoColor=white)](https://anthropic.com)

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat-square&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat-square&logo=redis&logoColor=white)](https://redis.io)
[![Qdrant](https://img.shields.io/badge/Qdrant-Vector_DB-FF4081?style=flat-square&logo=qdrant&logoColor=white)](https://qdrant.tech)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

<br/>

> **A production-grade agentic AI platform that autonomously resolves e-commerce support tickets**
> using event-driven architecture, LLM reasoning, vector search, and real-time streaming.

<br/>

</div>

---

## 🏗️ System Architecture

```mermaid
graph TB
    Browser["🌐 Browser UI\n(port 8000)"]
    API["⚡ FastAPI\n+ SSE Stream"]
    Redis["🔴 Redis\nPub/Sub"]
    Kafka["📨 Apache Kafka\n(Redpanda)"]
    Orch["🧠 Orchestrator\nLangGraph Workflow"]
    W1["👷 Worker 1"]
    W2["👷 Worker 2"]
    W3["👷 Worker 3"]
    MCP["🔌 MCP Server\nport 8001"]
    Claude["🤖 Claude\nSonnet 4.6"]
    Qdrant["🔍 Qdrant\nVector DB"]
    PG["🐘 PostgreSQL\nOrders · Customers"]

    Browser -->|"submit ticket"| API
    API -->|"ticket.created"| Kafka
    API <-->|"SSE events"| Redis
    Kafka -->|"consume"| Orch
    Orch -->|"agent.commands"| Kafka
    Kafka --> W1 & W2 & W3
    W1 & W2 & W3 -->|"agent.results"| Kafka
    Kafka -->|"results"| Orch
    W1 --> MCP & Claude & Qdrant
    MCP --> PG

    style Browser fill:#6366f1,color:#fff
    style API fill:#059669,color:#fff
    style Kafka fill:#1e1e2e,color:#fff
    style Orch fill:#7c3aed,color:#fff
    style Claude fill:#d97706,color:#fff
    style Qdrant fill:#e11d48,color:#fff
    style PG fill:#1d4ed8,color:#fff
    style Redis fill:#dc2626,color:#fff
    style MCP fill:#0891b2,color:#fff
```

---

## 🔄 Ticket Pipeline

```mermaid
sequenceDiagram
    actor Customer
    participant API as ⚡ FastAPI
    participant Kafka as 📨 Kafka
    participant Orch as 🧠 Orchestrator
    participant Worker as 👷 Worker
    participant Claude as 🤖 Claude LLM
    participant MCP as 🔌 MCP / PostgreSQL
    participant Qdrant as 🔍 Qdrant RAG

    Customer->>API: Submit ticket
    API->>Kafka: ticket.created event
    Kafka->>Orch: Consume ticket

    Orch->>Kafka: triage_ticket command
    Kafka->>Worker: Execute
    Worker->>Claude: Classify ticket
    Claude-->>Worker: category · priority
    Worker->>Kafka: agent.results
    Kafka->>Orch: triage done

    par Parallel Data Fetch
        Orch->>Kafka: get_order command
        Kafka->>Worker: Execute
        Worker->>MCP: Query PostgreSQL
        MCP-->>Worker: order data
    and
        Orch->>Kafka: get_shipment command
        Kafka->>Worker: Execute
        Worker->>MCP: Query PostgreSQL
        MCP-->>Worker: shipment data
    and
        Orch->>Kafka: get_customer command
        Kafka->>Worker: Execute
        Worker->>MCP: Query PostgreSQL
        MCP-->>Worker: customer data
    end

    Orch->>Kafka: retrieve_policy command
    Kafka->>Worker: Execute
    Worker->>Qdrant: Semantic search
    Qdrant-->>Worker: Top-3 policy passages

    Orch->>Kafka: resolve_ticket command
    Kafka->>Worker: Execute
    Worker->>Claude: Reason over facts + policy
    Claude-->>Worker: action · confidence 0.92

    Orch->>Kafka: generate_response command
    Kafka->>Worker: Execute
    Worker->>Claude: Write customer reply
    Claude-->>Worker: Personalised response

    Orch->>Orch: Quality Check (confidence ≥ 0.75?)
    Orch-->>API: ✅ APPROVED → deliver
    API-->>Customer: Real-time SSE response
```

---

## 🤖 Resolution Decision Tree

```mermaid
flowchart TD
    Start([🎫 Ticket Received]) --> Triage[🎯 LLM Triage\nClassify category & priority]
    Triage --> Missing{Required info\nmissing?}
    Missing -->|Yes| Escalate1[⚠️ Escalate\nAsk for details]

    Missing -->|No| Fetch[⚡ Parallel Fetch\nOrder · Shipment · Customer]
    Fetch --> RAG[📚 RAG Policy Search\nQdrant semantic search]
    RAG --> LLM[🤖 Claude Resolution\nReason over facts + policies]

    LLM --> R1{Express shipping\n+ delay > 3 days?}
    R1 -->|Yes| A1[💰 refund_shipping_fee\nconfidence 0.92]

    R1 -->|No| R2{Duplicate\ncharge?}
    R2 -->|Yes| A2[🔄 reverse_duplicate_charge\nconfidence 0.95]

    R2 -->|No| R3{Damaged or\nwrong item?}
    R3 -->|Yes| A3[📦 send_replacement\nconfidence 0.88]

    R3 -->|No| R4{Policy\nquestion?}
    R4 -->|Yes| A4[ℹ️ provide_information\nconfidence 0.85]

    R4 -->|No| R5{Standard delay\nor ambiguous?}
    R5 -->|Delay| A5[📍 provide_information\nconfidence 0.82]
    R5 -->|Ambiguous| A6[🔁 follow_up\nconfidence 0.65]

    A1 & A2 & A3 & A4 & A5 --> QC{Quality Check\nconfidence ≥ 0.75?}
    A6 --> Escalate2[⚠️ Escalate\nSpecialist team 24h]

    QC -->|✅ Pass| Deliver[🎉 Deliver to Customer\nReal-time via SSE]
    QC -->|❌ Fail| Escalate2

    style Start fill:#6366f1,color:#fff
    style Deliver fill:#059669,color:#fff
    style Escalate1 fill:#dc2626,color:#fff
    style Escalate2 fill:#dc2626,color:#fff
    style A1 fill:#d97706,color:#fff
    style A2 fill:#d97706,color:#fff
    style A3 fill:#d97706,color:#fff
    style A4 fill:#0891b2,color:#fff
    style A5 fill:#0891b2,color:#fff
```

---

## 📨 Kafka Event Topology

```mermaid
graph LR
    API["⚡ FastAPI"] -->|publish| TC["📥 ticket.created"]
    TC -->|consume| ORCH["🧠 Orchestrator"]
    ORCH -->|publish| AC["📤 agent.commands"]
    AC -->|consume| W["👷 Workers\n×3 parallel"]
    W -->|publish| AR["📥 agent.results"]
    AR -->|consume| ORCH
    ORCH -->|publish| DONE["✅ ticket.completed"]
    ORCH -->|publish| ESC["⚠️ ticket.escalated"]
    W -->|on error| RETRY["🔁 agent.commands.retry"]
    W -->|max retries| DLQ["💀 agent.commands.dlq"]

    style TC fill:#1e1e2e,color:#a6e3a1
    style AC fill:#1e1e2e,color:#89b4fa
    style AR fill:#1e1e2e,color:#fab387
    style DONE fill:#1e1e2e,color:#a6e3a1
    style ESC fill:#1e1e2e,color:#f38ba8
    style RETRY fill:#1e1e2e,color:#f9e2af
    style DLQ fill:#1e1e2e,color:#f38ba8
```

---

## 🗂️ Project Structure

```
novasupport/
├── 🤖 agents/
│   ├── triage.py          # LLM ticket classification
│   ├── resolver.py        # LLM resolution (6 action types)
│   └── responder.py       # LLM customer reply generation
│
├── 📱 apps/
│   ├── api/               # FastAPI + SSE real-time streaming
│   ├── orchestrator/      # LangGraph multi-step workflow
│   └── worker/            # Kafka consumers + task handlers
│
├── 📚 rag/
│   ├── indexer.py         # Chunk → embed → upsert to Qdrant
│   ├── retriever.py       # Semantic search via httpx
│   └── embedder.py        # all-MiniLM-L6-v2 (384-dim)
│
├── 🔌 mcp_servers/        # MCP protocol server for PostgreSQL
├── 📡 streaming/          # Kafka schemas, producer, consumer
├── 🏗️ infrastructure/     # Docker, Kafka topics, Prometheus
├── 📊 observability/      # Logging + metrics
└── 🧪 tests/              # Unit + integration + evaluation
```

---

## ⚡ Quick Start

### 1 — Start Infrastructure

```bash
docker compose up -d redpanda postgres redis qdrant
```

### 2 — First-time Setup

```bash
python scripts/seed_database.py              # Seed PostgreSQL (20 customers, 50 orders)
python infrastructure/kafka/create_topics.py # Create 7 Kafka topics
python -m rag.indexer                        # Index 73 policy chunks into Qdrant
```

### 3 — Launch Services

```bash
# Terminal 1 — MCP Server
python -m mcp_servers.operational_data.server

# Terminal 2 — API + UI  
uvicorn apps.api.main:app --reload

# Terminal 3 — Orchestrator
python -m apps.orchestrator

# Terminal 4 — Workers
python -m apps.worker --id worker-1 --count 3
```

### 4 — Open Browser

| URL | Description |
|-----|-------------|
| `http://localhost:8000` | 🎫 Support ticket UI |
| `http://localhost:8080` | 📊 Kafka topic inspector |

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| 🤖 AI Reasoning | Claude Sonnet 4.6 | Triage · Resolve · Respond |
| 🧠 Orchestration | LangGraph | Stateful multi-step workflow |
| 🔍 Embeddings | all-MiniLM-L6-v2 | 384-dim policy embeddings |
| 📨 Event Bus | Kafka (Redpanda) | Decoupled agent communication |
| ⚡ API | FastAPI + SSE | Real-time browser streaming |
| 🔌 Data Protocol | MCP | Tool-based PostgreSQL access |
| 🐘 Operational DB | PostgreSQL 16 | Orders · Customers · Payments |
| 🔍 Vector DB | Qdrant 1.9 | Policy RAG (73 chunks) |
| 🔴 Cache | Redis 7 | SSE pub/sub + idempotency |
| 📊 Monitoring | Prometheus + Grafana | Metrics · Dashboards |

---

<div align="center">

Built with ❤️ using **Claude Sonnet 4.6** · **LangGraph** · **Apache Kafka** · **FastAPI** · **PostgreSQL** · **Qdrant** · **Redis**

⭐ Star this repo if you find it useful!

</div>
