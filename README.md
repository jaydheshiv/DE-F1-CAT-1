# 🏎️ F1 Data Engineering Project — CAT 1

[![CI/CD Pipeline](https://github.com/jaydheshiv/DE-F1-CAT-1/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/jaydheshiv/DE-F1-CAT-1/actions/workflows/ci-cd.yml)
[![PR Quality Gate](https://github.com/jaydheshiv/DE-F1-CAT-1/actions/workflows/pr-check.yml/badge.svg)](https://github.com/jaydheshiv/DE-F1-CAT-1/actions/workflows/pr-check.yml)

> A full-stack **Formula 1 Data Engineering** pipeline built for the Data Engineering Lab (CAT 1).  
> Covers batch ingestion, real-time Kafka streaming, Airflow orchestration, FastAPI backend, and a React/Vite dashboard.

---

## 🏗️ Architecture

```
┌─────────────┐    ┌──────────┐    ┌──────────────┐    ┌────────────┐
│  F1 CSV DB  │───▶│ Producer │───▶│ Kafka Topic  │───▶│  Consumer  │
│  (Raw Data) │    │(Streaming│    │f1-lap-events │    │(Postgres)  │
└─────────────┘    └──────────┘    └──────────────┘    └────────────┘
                                                               │
┌─────────────┐    ┌──────────┐    ┌──────────────┐           │
│   Frontend  │◀───│ FastAPI  │◀───│  PostgreSQL  │◀──────────┘
│ (React/Vite)│    │  (REST)  │    │   (labdb)    │
└─────────────┘    └──────────┘    └──────────────┘
                                          ▲
                                   ┌──────────────┐
                                   │   Airflow    │
                                   │  (Scheduler) │
                                   └──────────────┘
```

## 📦 Services

| Service | Tech | Port | Description |
|---------|------|------|-------------|
| `postgres` | PostgreSQL 15 | 5432 | Primary data store |
| `kafka` | Confluent Kafka 7.6 | 9092 | Real-time event streaming |
| `zookeeper` | Confluent ZooKeeper | 2181 | Kafka coordination |
| `producer` | Python | — | Streams F1 lap events to Kafka |
| `consumer` | Python | — | Consumes events, writes to Postgres |
| `airflow-webserver` | Apache Airflow 2.9 | 8080 | Workflow UI |
| `airflow-scheduler` | Apache Airflow 2.9 | — | DAG scheduler |
| `api` | FastAPI + Uvicorn | 8000 | REST API |
| `frontend` | React + Vite | 5173 | Dashboard UI |

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop ≥ 24.x
- Docker Compose v2

### Run everything

```bash
# Clone the repo
git clone https://github.com/jaydheshiv/DE-F1-CAT-1.git
cd DE-F1-CAT-1

# Place your F1 CSV data inside a folder named "f1 db/" at the root
# (excluded from git due to size)

# Start all services
docker compose up -d --build

# Check status
docker compose ps
```

### Access Services

| UI | URL |
|----|-----|
| Frontend Dashboard | http://localhost:5173 |
| FastAPI Docs | http://localhost:8000/docs |
| Airflow | http://localhost:8080 (admin/admin) |

---

## 🔄 CI/CD Pipeline

This project uses **GitHub Actions** for automated CI/CD.

### Workflows

#### `ci-cd.yml` — Main Pipeline (push to `main`/`develop`)
```
lint → test-api → test-frontend → build-docker → integration-test
```

| Stage | What it does |
|-------|-------------|
| 🔍 **Lint** | flake8 + black checks on Python code |
| 🧪 **Test API** | Spins up Postgres, installs deps, smoke-tests imports |
| 🖥️ **Test Frontend** | `npm ci` + `npm run build`, uploads dist artifact |
| 🐳 **Build Docker** | Builds & pushes 4 images to GitHub Container Registry |
| 🔗 **Integration** | `docker compose up` end-to-end health check (main only) |

#### `pr-check.yml` — Pull Request Gate
Runs on every PR: lint, docker-compose validation, YAML validation, secret scanning.

### Docker Images (GHCR)
```
ghcr.io/jaydheshiv/de-f1-cat-1-api:latest
ghcr.io/jaydheshiv/de-f1-cat-1-frontend:latest
ghcr.io/jaydheshiv/de-f1-cat-1-producer:latest
ghcr.io/jaydheshiv/de-f1-cat-1-consumer:latest
```

---

## 📁 Project Structure

```
DE-F1-CAT-1/
├── .github/
│   └── workflows/
│       ├── ci-cd.yml          # Main CI/CD pipeline
│       └── pr-check.yml       # PR quality gate
├── airflow/
│   └── dags/                  # Airflow DAG definitions
├── api/
│   ├── main.py                # FastAPI application
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   ├── src/                   # React components
│   ├── package.json
│   └── Dockerfile
├── streaming/
│   ├── producer.py            # Kafka producer
│   ├── consumer.py            # Kafka consumer
│   ├── Dockerfile.producer
│   └── Dockerfile.consumer
├── pipeline/
│   ├── preprocessing/
│   ├── feature_engineering/
│   └── validation/
├── sql/
│   └── init.sql               # DB schema
├── docker-compose.yml
└── README.md
```

---

## 👨‍💻 Author

**Jaydheshiv** — Data Engineering Lab, Semester 9

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
