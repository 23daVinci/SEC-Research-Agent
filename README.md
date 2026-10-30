# Large-scale agentic RAG

Platform for a production-quality agentic RAG system. This repo currently contains the
**infrastructure layer**: Docker Compose, Airflow for pipeline orchestration, and Loki for logs.

## Architecture

```
                 ┌──────────────── Docker network "agentic-rag" ────────────────┐
                 │                                                               │
  dags/ ───────▶ │  airflow-dag-processor ─┐                                     │
                 │  airflow-scheduler ─────┼──▶ postgres (Airflow metadata)      │
                 │  airflow-triggerer ─────┤                                     │
  browser ─────▶ │  airflow-apiserver ◀────┘   (UI, REST, task Execution API)    │
                 │                                                               │
                 │  every container ──stdout──▶ alloy ──push──▶ loki ◀── grafana │ ◀── browser
                 └───────────────────────────────────────────────────────────────┘
```

| Service | Role | Host URL / port |
|---|---|---|
| `postgres` | Airflow metadata DB | `localhost:5433` |
| `airflow-apiserver` | Airflow UI and API | http://localhost:8081 |
| `airflow-scheduler`, `-dag-processor`, `-triggerer` | Scheduling, DAG parsing, deferred tasks | none |
| `airflow-init` | One-shot DB migration, exits when done | none |
| `loki` | Log storage (7-day retention) | http://localhost:3100/ready |
| `alloy` | Collects container logs and ships them to Loki | http://localhost:12345 |
| `grafana` | Log exploration and dashboards | http://localhost:3000 |

All ports are bound to `127.0.0.1` only. The host-side ports (5433, 8081) are not the defaults,
to avoid clashing with a local Postgres and another Airflow; change them in `infra/.env`.

## Quickstart

Requires Docker with Compose v2. From the repo root:

```powershell
# Windows (PowerShell)
.\infra\scripts\bootstrap.ps1
cd infra
docker compose up -d --wait
```

```bash
# macOS / Linux / Git Bash
./infra/scripts/bootstrap.sh
cd infra && docker compose up -d --wait
```

`bootstrap` creates `infra/.env` with generated secrets. It is safe to re-run and never changes
values that are already set. The first `up` builds the Airflow image, which takes a few minutes.

Then:

- **Airflow:** http://localhost:8081 (no login in dev). New DAGs are created *paused*; unpause
  one before triggering it or its runs will sit in `queued`.
- **Grafana:** http://localhost:3000, user `admin`, password is `GRAFANA_ADMIN_PASSWORD` in
  `infra/.env`. The **Agentic RAG / Logs Overview** dashboard is provisioned automatically.
  Use **Explore** for ad-hoc queries.

Try the smoke-test DAG (`dags/hello.py`, which fails one task on purpose), then in Grafana Explore:

```logql
{service="airflow-scheduler", level="warning"} | json | dag_id="hello"
sum by (service) (count_over_time({level="error"}[1h]))
```

## Everyday commands

Run from `infra/`. `COMPOSE_PROFILES` in `.env` makes these cover the whole stack.

| Task | Command |
|---|---|
| Start everything | `docker compose up -d --wait` |
| Status and health | `docker compose ps` |
| Follow logs | `docker compose logs -f airflow-scheduler` |
| Airflow CLI | `docker compose exec airflow-scheduler airflow dags list` |
| Rebuild Airflow after editing `requirements.txt` | `docker compose build airflow-init && docker compose up -d` |
| Stop (keeps data) | `docker compose down` |
| Wipe everything incl. DB and logs | `docker compose down -v` |
| Only Postgres + Airflow | `COMPOSE_PROFILES=airflow docker compose up -d` (bash) |

After a reboot or Docker Desktop restart the containers stay stopped; run `docker compose up -d` again.
Data lives in named volumes and survives.

## Layout

```
dags/                       Airflow DAGs (bind-mounted; new files picked up within ~30s)
plugins/                    Airflow plugins
logs/                       Airflow task log files (gitignored)
infra/
  docker-compose.yml        entry point; includes the files below
  compose/core.yml          network, Postgres
  compose/airflow.yml       Airflow services (profile: airflow)
  compose/observability.yml Loki, Alloy, Grafana (profile: observability)
  airflow/                  Dockerfile + requirements.txt for the custom image
  observability/            Loki, Alloy and Grafana config, datasource and dashboards
  scripts/                  bootstrap.ps1 / bootstrap.sh
  .env.example              documented settings; copy to .env (gitignored)
```

## How logging works

Containers write JSON to stdout (`AIRFLOW__LOGGING__JSON_LOGS`). Alloy discovers containers of
this Compose project through the Docker socket, labels each stream with `service` (and `level`
for JSON lines), and pushes to Loki. Task logs reach Loki through the scheduler's stdout.

Keep **labels low-cardinality** (`service`, `level`, `env`). Put identifiers such as `dag_id`,
`task_id` and `run_id` in the log body and filter them at query time with `| json`.

## Dev-only settings to change before exposing this anywhere

- Airflow runs with `SIMPLE_AUTH_MANAGER_ALL_ADMINS=true` (no login; fine on localhost).
- Executor is `LocalExecutor` with `parallelism=8`; use Celery for real scale.
- Loki stores on local disk; use object storage for production.
- Alloy mounts the Docker socket read-only; treat that as privileged access.
- Memory limits are sized for a laptop (Docker Desktop with ~7.5 GB).

## Troubleshooting

- **"port is already allocated" / "bind: forbidden"**: another process holds the host port. Change
  the matching `*_PORT` in `infra/.env`, then `docker compose up -d`.
- **DAG run stuck in `queued`**: the DAG is paused. Unpause it in the UI or with `airflow dags unpause <id>`.
- **New DAG file not showing**: wait ~30s (`REFRESH_INTERVAL`), then check `docker compose logs airflow-dag-processor`.
- **Changed `POSTGRES_PASSWORD` and Postgres rejects it**: it is only read on first init of an empty
  volume; use `docker compose down -v` to reinitialize (this deletes data).
- **No logs in Grafana**: check `curl localhost:3100/ready` and `docker compose logs alloy`.
