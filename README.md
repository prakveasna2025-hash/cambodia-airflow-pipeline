# \# Cambodia Data Warehouse Pipeline

# 

# An end-to-end data pipeline practicing \*\*Apache Airflow orchestration\*\* — from raw CSV files through PostgreSQL, dbt transformations, and a Streamlit dashboard.

# 

# Built as a learning project to develop muscle memory for the shape of real Data Engineering work: extraction, validation, dimensional modelling, testing, orchestration, and presentation.

# 

# \---

# 

# \## What this demonstrates

# 

# | Layer | Tools | What it shows |

# |---|---|---|

# | \*\*Extraction\*\* | Python, pandas, SQLAlchemy | Two source CSVs → PostgreSQL raw schema |

# | \*\*Validation\*\* | pandas, custom validators | Pre-load and post-load checks, pinned defect counts |

# | \*\*Warehouse\*\* | PostgreSQL (Docker) | Named volume, health check, host port 5434 |

# | \*\*Transformations\*\* | dbt | Staging → intermediate → marts, 67 tests |

# | \*\*Dimensional model\*\* | Kimball star schema | `dim\_province`, `dim\_date`, `dim\_indicator`, `fact\_population`, `fact\_exchange\_rate` |

# | \*\*Orchestration\*\* | Apache Airflow 3.3.2 | Fan-out / fan-in DAG, retries, exponential backoff |

# | \*\*Dashboard\*\* | Streamlit | KPIs, trends, filters — reading from dbt marts |

# | \*\*Testing\*\* | pytest, dbt tests | Unit tests on validators, dbt structural + business-rule tests |

# | \*\*CI-ready\*\* | Docker Compose | One-command stack start, reproducible environment |

# 

# \---

# 

# \## Architecture

# 

# ```mermaid

# flowchart LR

# &#x20;   subgraph Sources

# &#x20;       A1\[population\_by\_province<br/>2024 CSV]

# &#x20;       A2\[khmer\_riel\_usd\_exchange\_rate<br/>2003-2023 CSV]

# &#x20;   end

# 

# &#x20;   subgraph Extractors\["Python extractors"]

# &#x20;       E1\[extract\_population.py]

# &#x20;       E2\[extract\_exchange\_rate.py]

# &#x20;   end

# 

# &#x20;   subgraph Raw\["PostgreSQL · raw schema"]

# &#x20;       R1\[(raw.population\_by\_province<br/>25 rows)]

# &#x20;       R2\[(raw.exchange\_rate<br/>7,531 rows)]

# &#x20;   end

# 

# &#x20;   subgraph DBT\["dbt · dbt\_dev\_staging / dbt\_dev\_intermediate / dbt\_dev\_marts"]

# &#x20;       S1\[stg\_population<br/>stg\_province<br/>stg\_exchange\_rate]

# &#x20;       I1\[int\_population\_by\_province]

# &#x20;       M1\[(dim\_province<br/>dim\_date<br/>dim\_indicator)]

# &#x20;       M2\[(fact\_population<br/>fact\_exchange\_rate)]

# &#x20;       M3\[(province\_year\_summary)]

# &#x20;   end

# 

# &#x20;   subgraph Presentation

# &#x20;       D1\[Streamlit Dashboard]

# &#x20;       A3\[Airflow DAG<br/>cambodia\_pipeline]

# &#x20;   end

# 

# &#x20;   A1 --> E1

# &#x20;   A2 --> E2

# &#x20;   E1 --> R1

# &#x20;   E2 --> R2

# &#x20;   R1 --> S1

# &#x20;   R2 --> S1

# &#x20;   S1 --> I1

# &#x20;   S1 --> M1

# &#x20;   S1 --> M2

# &#x20;   I1 --> M3

# &#x20;   M1 --> D1

# &#x20;   M2 --> D1

# &#x20;   M3 --> D1

# &#x20;   A3 -.orchestrates.-> E1

# &#x20;   A3 -.orchestrates.-> E2

# ```

# 

# \---

# 

# \## Data model (star schema)

# 

# ```mermaid

# erDiagram

# &#x20;   dim\_province ||--o{ fact\_population : "province\_key"

# &#x20;   dim\_date     ||--o{ fact\_exchange\_rate : "date\_key"

# &#x20;   dim\_indicator {

# &#x20;       int indicator\_key PK

# &#x20;       text indicator\_code

# &#x20;       text indicator\_name

# &#x20;       text indicator\_unit

# &#x20;       text indicator\_grain

# &#x20;       text indicator\_source\_table

# &#x20;   }

# &#x20;   dim\_province {

# &#x20;       int province\_key PK

# &#x20;       text province\_code UK

# &#x20;       text province\_name UK

# &#x20;   }

# &#x20;   dim\_date {

# &#x20;       int date\_key PK

# &#x20;       date date UK

# &#x20;       smallint year

# &#x20;       smallint quarter

# &#x20;       smallint month

# &#x20;       text month\_name

# &#x20;       smallint day

# &#x20;       smallint day\_of\_week

# &#x20;       text day\_name

# &#x20;       boolean is\_weekend

# &#x20;       boolean is\_month\_start

# &#x20;       boolean is\_month\_end

# &#x20;   }

# &#x20;   fact\_population {

# &#x20;       int province\_key FK

# &#x20;       text province\_code

# &#x20;       text province\_name

# &#x20;       smallint snapshot\_year

# &#x20;       bigint population

# &#x20;   }

# &#x20;   fact\_exchange\_rate {

# &#x20;       int date\_key FK

# &#x20;       date date

# &#x20;       bigint purchase

# &#x20;       bigint sale

# &#x20;       double midpoint

# &#x20;       double exchange\_rate

# &#x20;       double spread\_value

# &#x20;   }

# &#x20;   province\_year\_summary {

# &#x20;       int province\_key

# &#x20;       text province\_code

# &#x20;       text province\_name

# &#x20;       smallint year

# &#x20;       bigint population\_total

# &#x20;   }

# ```

# 

# \---

# 

# \## Orchestration

# 

# The Airflow DAG runs two extract tasks in parallel, then validates the warehouse:

# 

# !\[DAG graph](docs/screenshots/dag\_graph.png)

# 

# ```mermaid

# flowchart LR

# &#x20;   P\[extract\_population] --> V\[validate\_warehouse]

# &#x20;   E\[extract\_exchange\_rate] --> V

# ```

# 

# \- \*\*Parallel extracts\*\* — neither waits for the other

# \- \*\*Fan-in validation\*\* — waits for both, skips if either fails

# \- \*\*Retries\*\* — 2 retries with exponential backoff (1min → 2min → 4min, capped at 10min)

# \- \*\*Schedule\*\* — manual trigger only (`schedule=None`), `catchup=False`

# 

# \---

# 

# \## Data quality — dual-layer testing

# 

# | Layer | Where | What it checks |

# |---|---|---|

# | \*\*DataFrame validation\*\* (`scripts/validators.py`) | Inside extractors, before load | Shape, nulls, duplicates, contiguity, exact sums |

# | \*\*Warehouse health\*\* (`scripts/validate\_warehouse.py`) | Post-load, on DB state | Schema contracts, row counts, reconciliation, FK joins, nulls, duplicates |

# | \*\*dbt tests\*\* | On dbt models | 67 tests: not\_null, unique, accepted\_values, relationships, custom business rules |

# | \*\*Unit tests\*\* (`tests/test\_validators.py`) | pytest, no DB | 12 tests proving each validator catches its specific defect |

# 

# \*\*Known source defects are pinned at expected counts, not silently ignored:\*\*

# 

# \- 16 inverted bid/ask rows in exchange rates (`purchase > sale`)

# \- 49 missing days (April 2008 fully absent + scattered)

# \- 2,109 midpoint rounding inconsistencies

# 

# These are documented in \[`docs/source-notes.md`](docs/source-notes.md) and enforced via `EXPECTED\_INVERTED\_ROWS = 16`, `EXPECTED\_MISSING\_DAYS = 49` in `scripts/validators.py`. \*\*Not in dbt tests\*\* — dbt tests must always pass; defect assertions belong in validators.

# 

# \---

# 

# \## Data sources

# 

# | Source | Rows | Range | Notes |

# |---|---|---|---|

# | Population by province (Cambodia, 2024) | 25 | 2024 snapshot | Clean — verified Day 2 |

# | USD/KHR exchange rate (NBC) | 7,531 | 2003-04-01 → 2023-12-31 | Documented defects — see \[`docs/source-notes.md`](docs/source-notes.md) |

# 

# \---

# 

# \## Stack

# 

# \- \*\*Python 3.13\*\* (managed with `uv`)

# \- \*\*PostgreSQL 16\*\* (Docker, named volume)

# \- \*\*dbt-core + dbt-postgres 1.12\*\* (staging, intermediate, marts layers)

# \- \*\*Apache Airflow 3.3.2\*\* (Docker Compose, CeleryExecutor)

# \- \*\*Streamlit\*\* (dashboard)

# \- \*\*pytest\*\* (validator unit tests)

# 

# \---

# 

# \## How to run

# 

# \### Prerequisites

# 

# \- Docker Desktop running

# \- `uv` installed (`powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`)

# 

# \### Configure the environment (first time only)

# 

# \*\*This step is required on a fresh clone.\*\* Two `.env` files hold configuration and secrets that are intentionally not committed. Create them from the provided templates:

# 

# ```powershell

# \# Warehouse credentials

# Copy-Item docker\\.env.example docker\\.env

# 

# \# Airflow config + Fernet key + warehouse URL

# Copy-Item docker\\airflow\\.env.example docker\\airflow\\.env

# ```

# 

# \*\*Generate a Fernet key\*\* and paste it into `docker\\airflow\\.env` (replace `FERNET\_KEY=CHANGE\_ME`):

# 

# ```powershell

# docker run --rm python:3.13-slim sh -c "pip install cryptography -q \&\& python -c 'from cryptography.fernet import Fernet; print(Fernet.generate\_key().decode())'"

# ```

# 

# \*\*Create the dbt profile\*\* at `\~/.dbt/profiles.yml` (outside the repo — contains the DB password):

# 

# ```powershell

# New-Item -ItemType Directory -Force "$HOME\\.dbt" | Out-Null

# notepad "$HOME\\.dbt\\profiles.yml"

# ```

# 

# Paste:

# 

# ```yaml

# cambodia\_dw:

# &#x20; target: dev

# &#x20; outputs:

# &#x20;   dev:

# &#x20;     type: postgres

# &#x20;     host: 127.0.0.1

# &#x20;     port: 5434

# &#x20;     user: airflow

# &#x20;     password: changeme

# &#x20;     dbname: airflow

# &#x20;     schema: dbt\_dev

# &#x20;     threads: 4

# ```

# 

# \*\*Why `127.0.0.1:5434` in `profiles.yml` but `cambodia\_dw\_postgres:5432` in `docker/airflow/.env`?\*\*

# 

# \- dbt runs on your \*\*host machine\*\* → uses the \*\*host-mapped\*\* port `5434`

# \- Airflow runs \*\*inside Docker\*\* → uses the \*\*container DNS name\*\* and internal port `5432`

# 

# \### 1 — Start the stack

# 

# ```powershell

# \# Warehouse Postgres

# docker compose -f docker/docker-compose.yml up -d

# 

# \# Airflow (7 services)

# docker compose -f docker/airflow/docker-compose.yaml up -d

# 

# \# Wait \~60s, then check

# docker ps --format "table {{.Names}}\\t{{.Status}}"

# ```

# 

# \### 2 — Build the warehouse

# 

# ```powershell

# uv sync

# 

# uv run dbt seed --project-dir cambodia\_dw

# uv run dbt run  --project-dir cambodia\_dw

# uv run dbt test --project-dir cambodia\_dw

# ```

# 

# \### 3 — Trigger the pipeline

# 

# \*\*Option A — Airflow UI:\*\*

# \- Open `http://localhost:8080` (login `airflow` / `airflow`)

# \- Find `cambodia\_pipeline` → click the play button

# 

# \*\*Option B — CLI:\*\*

# ```powershell

# docker exec airflow-airflow-scheduler-1 airflow dags trigger cambodia\_pipeline

# ```

# 

# \### 4 — Open the dashboard

# 

# ```powershell

# uv run streamlit run dashboard/app.py

# ```

# 

# Opens at `http://localhost:8501`.

# 

# \---

# 

# \## Project layout

# 

# ```

# cambodia-airflow-pipeline/

# ├── dags/                          # Airflow DAG files

# ├── scripts/                       # Python extraction + validation

# ├── sql/                           # SQL scripts and analysis

# ├── data/raw/                      # Source CSVs

# ├── cambodia\_dw/                   # dbt project

# │   ├── seeds/                     # province\_reference, indicator\_reference

# │   ├── models/

# │   │   ├── staging/               # stg\_\* views

# │   │   ├── intermediate/          # int\_\* views

# │   │   └── marts/                 # dim\_\*, fact\_\*, summary

# │   └── tests/                     # Custom SQL tests

# ├── tests/                         # pytest unit tests for validators

# ├── dashboard/                     # Streamlit app

# ├── docs/                          # Source notes, devlog, screenshots

# ├── docker/                        # Compose files for warehouse + Airflow

# └── pyproject.toml                 # uv project config

# ```

# 

# \---

# 

# \## Key design decisions

# 

# | Decision | Rationale |

# |---|---|

# | \*\*Load raw as-is\*\* | Raw layer is a faithful copy of source. Cleaning belongs in dbt. |

# | \*\*TRUNCATE + INSERT\*\* (not `DROP + CREATE`) | dbt staging views depend on raw tables. `TRUNCATE` preserves structure. |

# | \*\*Canonical seeds, not derived dims\*\* | `dim\_province` seeded from `province\_reference.csv`, not `SELECT DISTINCT`. |

# | \*\*Explicit surrogate keys in seeds\*\* | `ROW\_NUMBER` renumbers on insert, breaking FKs silently. |

# | \*\*Staging = views, marts = tables\*\* | Staging is cheap to recompute; marts are joined repeatedly. |

# | \*\*Blue/green migration\*\* | New dimensions built alongside old, verified, then old retired. |

# | \*\*Defects pinned at expected counts\*\* | Silent drift is the enemy. Documented, asserted, tolerant. |

# 

# \---

# 

# \## Testing philosophy

# 

# \*\*Four layers, each catching something the others can't:\*\*

# 

# 1\. \*\*Unit tests\*\* — pure functions on DataFrames, no DB

# 2\. \*\*DataFrame validation\*\* — inside the extractor, fail-fast before writing

# 3\. \*\*Warehouse health\*\* — post-load, structural integrity of the actual DB

# 4\. \*\*dbt tests\*\* — on the transformation layer, structural + business rules

# 

# \*\*Test failures must be rare and meaningful.\*\* Warnings that always appear get ignored.

# 

# \---

# 

# \## Interview-ready highlights

# 

# \- \*\*Fan-out / fan-in DAG\*\* with exponential backoff retries

# \- \*\*Three-layer dbt model\*\* with custom schemas (`\_staging`, `\_intermediate`, `\_marts`)

# \- \*\*Two-endpoint connection strings\*\* — host `127.0.0.1:5434` vs. container `cambodia\_dw\_postgres:5432`

# \- \*\*Shared external network\*\* bridging Airflow and warehouse stacks

# \- \*\*Blue/green dimension migration\*\* with set-difference verification

# \- \*\*Defect-pinned validation\*\* — accepts known defects, catches drift

# \- \*\*Idempotent loads\*\* — TRUNCATE + INSERT preserves dependent views

# \- \*\*Structured logging\*\* with timestamps, levels, logger names

# \- \*\*67 dbt tests\*\* covering not\_null, unique, accepted\_values, relationships, business rules

# 

# \---

# 

# \## What's next

# 

# \- \[ ] Phase 2 of Day 19: retire old `dim.\*` tables, update `validate\_warehouse.py`

# \- \[ ] Add `dbt run` + `dbt test` as Airflow tasks

# \- \[ ] `dbt docs generate` + source freshness checks

# \- \[ ] CI: GitHub Actions running pytest + dbt test

# 

# \---

# 

# \## License

# 

# Practice project — no license. Use as reference.

