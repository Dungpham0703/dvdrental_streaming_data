# Real-Time E-Commerce Analytics Lakehouse

> **Architected & Implemented by:** Dung Pham
> **Repository:** [dvdrental_streaming_data](https://github.com/Dungpham0703/dvdrental_streaming_data)

![Data Platform Architecture](docs/images/data_platform_architect.png)

## 1. Business Requirement

Operational databases (like PostgreSQL) are optimized for transactions but struggle with heavy analytical queries, causing performance degradation and loss of historical data (e.g., customer address changes).

**Goal:** Build a Real-Time Data Lakehouse to transform 15 normalized transactional tables into an optimized Star Schema. This enables Data Analysts to run high-performance OLAP queries and visualize business metrics in near real-time without impacting the source system's performance.

## 2. Data Modeling & Solution

To solve the analytical bottlenecks, the data was remodeled into a **Star Schema** following a Medallion Architecture (Bronze -> Silver -> Gold).

```text
      ┌─────────────────────────┐             ┌─────────────────────────┐
      │      dim_customer       │             │        dim_date         │
      │ ----------------------- │             │ ----------------------- │
      │ (SCD Type 2 History)    │             │ (Time Context)          │
      │ PK: customer_key        │             │ PK: date_key            │
      │ valid_from, valid_to    │             │ day, month, year...     │
      └───────────┬─────────────┘             └─────────────┬───────────┘
                  │ 1:N                                     │ 1:N
                  │           ┌───────────────┐             │
                  └─────────► │  FACT TABLES  │ ◄───────────┘
                              │ ------------- │
                              │ fact_rental   │
                              │ fact_payment  │
                  ┌─────────► │               │ ◄───────────┐
                  │ 1:N       └───────────────┘       1:N   │
                  │                                         │
      ┌───────────┴─────────────┐             ┌─────────────┴───────────┐
      │        dim_film         │             │     dim_store_staff     │
      │ ----------------------- │             │ ----------------------- │
      │ (Movie Details)         │             │ (Location/Staff Info)   │
      │ PK: film_key            │             │ PK: store_key/staff_key │
      │ title, category, rating │             │ address, city, country  │
      └─────────────────────────┘             └─────────────────────────┘
```

- **Slowly Changing Dimensions (SCD Type 2):** Applied to dimensions like `dim_customer` to preserve a complete history of changes (using `valid_from`, `valid_to`, `is_current`). This ensures accurate time-travel reporting and prevents historical data loss.
- **Fact & Dimension Tables:** Complex multi-table joins are simplified into centralized Fact tables (`fact_rental`, `fact_payment`) surrounded by descriptive Dimensions, making analytics straightforward and performant.

## 3. Tool Selection & Distributed Architecture

```mermaid
flowchart TB
  subgraph SOURCE["1. SOURCE & CDC INGESTION LAYER"]
    direction TB
    RDS[("AWS RDS PostgreSQL 16<br/>(15 OLTP Tables)<br/>WAL Logical Replication")]
    CTRL_NODE["Control Node (EC2)<br/>IP: 172.31.16.120"]
    DEBEZIUM["Debezium Kafka Connect 2.7<br/>(Extract WAL / CDC Events)"]
    PRODUCER["Traffic Generator / Producer<br/>(Stateful Synthetic CDC Load)"]
    RDS -->|WAL Stream| DEBEZIUM
    PRODUCER -->|Simulate Orders/Rentals| RDS
    CTRL_NODE --- DEBEZIUM
    CTRL_NODE --- PRODUCER
  end

  subgraph KAFKA_CLUSTER["2. DISTRIBUTED MESSAGE BROKER (KRaft - 3 Nodes)"]
    direction LR
    KAFKA1["Kafka Broker/Controller 1<br/>172.31.16.100:9092"]
    KAFKA2["Kafka Broker/Controller 2<br/>172.31.16.101:9092"]
    KAFKA3["Kafka Broker/Controller 3<br/>172.31.16.102:9092"]
    KAFKA1 <-->|Quorum 9093| KAFKA2
    KAFKA2 <-->|Quorum 9093| KAFKA3
    KAFKA3 <-->|Quorum 9093| KAFKA1
  end

  DEBEZIUM -->|Publish CDC Topics<br/>Replication=3, Partitions=6| KAFKA_CLUSTER

  subgraph SPARK_CLUSTER["3. DISTRIBUTED STREAM PROCESSING (Spark Standalone Cluster)"]
    direction TB
    MASTER["Spark Master Node<br/>172.31.16.110:7077<br/>Web UI: 8080"]
    W1["Spark Worker 1<br/>172.31.16.111<br/>(2 Cores, 6GB)"]
    W2["Spark Worker 2<br/>172.31.16.112<br/>(2 Cores, 6GB)"]
    W3["Spark Worker 3<br/>172.31.16.113<br/>(2 Cores, 6GB)"]
    MASTER --- W1
    MASTER --- W2
    MASTER --- W3
    STREAMER["HoodieMultiTableStreamer<br/>(Apache Hudi 0.15 on Spark 3.5)"]
    W1 -.-> STREAMER
    W2 -.-> STREAMER
    W3 -.-> STREAMER
  end

  KAFKA_CLUSTER -->|Parallel Consumer Streams| SPARK_CLUSTER

  subgraph LAKEHOUSE["4. AWS S3 LAKEHOUSE STORAGE (dvd-rental-lakehouse)"]
    direction TB
    BRONZE["Bronze Layer (Raw CDC)<br/>Apache Hudi (Merge-On-Read - MOR)<br/>Continuous Upserts & Deletes"]
    SILVER["Silver Layer (Cleansed/Staging)<br/>Apache Iceberg<br/>Deduplicated, CDC Tombstones Handled"]
    GOLD["Gold Layer (Analytics Mart)<br/>Apache Iceberg (Star Schema)<br/>• fact_rental, fact_payment<br/>• dim_customer (SCD Type 2)<br/>• dim_film, dim_store_staff, dim_date"]

    BRONZE -->|dbt transformation| SILVER
    SILVER -->|dbt transformation| GOLD
  end

  STREAMER -->|Direct S3 Writes via VPC Endpoint| BRONZE

  subgraph ORCHESTRATION["5. ORCHESTRATION & TRANSFORMATION"]
    AIRFLOW["Apache Airflow 2.x<br/>(Running on Control Node)"]
    DBT["dbt (Data Build Tool)<br/>dbt-athena-adapter"]
    AIRFLOW -->|Cron */15 min<br/>Trigger & Monitor| DBT
    DBT -->|Execute SQL| ATHENA
  end

  subgraph QUERY_CATALOG["6. CATALOG & SERVERLESS QUERY ENGINE"]
    GLUE["AWS Glue Data Catalog<br/>(Schema Discovery & Partitions)"]
    ATHENA["AWS Athena (Trino/Presto Engine)<br/>Serverless Interactive SQL Queries"]
    GLUE --- ATHENA
    ATHENA <-->|Read / Write Metadata & Tables| LAKEHOUSE
  end

  subgraph SERVING["7. DATA SERVING & CONSUMPTION LAYER"]
    direction LR
    subgraph BI["BI Analytics"]
      METABASE["Metabase BI Engine<br/>4 Automated Dashboards:<br/>1. Revenue & Payment<br/>2. Rental Operations<br/>3. Customer Intelligence<br/>4. Film Inventory"]
    end
    subgraph GENAI["GenAI Chat-with-Data"]
      NEXTJS["Frontend AI Dashboard<br/>(Next.js + Tailwind CSS)"]
      AGENT["FastAPI AI Agent Backend<br/>(Google Gemini Pro)"]
      USER(["Business Users"])
      USER <-->|Natural Language Prompt| NEXTJS
      NEXTJS <-->|REST API| AGENT
      AGENT -->|Text-to-SQL| ATHENA
    end
    ATHENA -->|SQL Results| METABASE
  end
```

The lakehouse is deployed as a **Distributed Multi-Node Architecture** on AWS via Terraform:

- **Source & CDC Layer:** **AWS RDS PostgreSQL 16** with Write-Ahead Logging (WAL) and `REPLICA IDENTITY FULL`. **Debezium Kafka Connect 2.7** runs on a dedicated Control Node (`172.31.16.120`) alongside the Traffic Producer to extract zero-impact CDC streams without locking source tables.
- **Distributed Message Broker (KRaft 3-Node Cluster):** **Apache Kafka 3.5.1** runs on 3 independent EC2 nodes (`172.31.16.100`, `172.31.16.101`, `172.31.16.102`) in KRaft consensus mode (no Zookeeper). Configured with **Replication Factor = 3**, `min.insync.replicas = 2`, and **6 partitions** per CDC topic for high availability and throughput.
- **Distributed Stream Processing (Spark 1 Master + 3 Workers):** **Apache Spark 3.5.0 Standalone Cluster** (1 Master at `172.31.16.110:7077` and 3 Workers with 6 cores / 18 GB aggregate RAM). Runs `HoodieMultiTableStreamer` to continuously consume streams across Kafka brokers and write into the Bronze layer in parallel.
- **Apache Hudi (Bronze/Raw Layer):** Stores raw CDC data as Hudi Merge-On-Read (MOR) tables in AWS S3 (`dvd-rental-lakehouse`) via high-speed S3 VPC Gateway Endpoint, enabling ultra-low-latency upserts and deletes.
  ![Raw Data with Hudi Format](docs/images/raw_data_with_hudi_format.png)
- **Apache Iceberg (Silver/Staging & Gold/Mart Layers):** Chosen for Silver and Gold layers. Iceberg delivers high-performance OLAP queries, schema evolution, and time-travel querying. It integrates seamlessly with dbt and AWS Athena for analytical reads.
  ![Staging Data with Iceberg Format](docs/images/staging_data_with_iceberg_format.png)
  ![Mart Data with Iceberg Format](docs/images/mart_data_with_iceberg_format.png)
- **dbt (Data Build Tool):** Manages SQL transformations from Raw Hudi to Staging Iceberg to Mart Star Schema. Handles complex SCD Type 2 logic, deduplication, incremental CDC processing, and automated data quality testing.
  ![dbt Lineage Graph](docs/images/dbt_linage_graph_model.png)
- **Apache Airflow (Orchestration):** Orchestrates dbt workflows on the Control Node. Runs on a 15-minute cron schedule (`*/15 * * * *`), automatically executing `dbt deps` -> `dbt run` -> `dbt test` with Slack alerts and AWS Secrets Manager integration.
- **AWS Athena & Glue Data Catalog:** Provides serverless cataloging and querying across S3 Iceberg tables without requiring persistent compute clusters.
  ![Databases on AWS Glue](docs/images/databases_on_aws_glue.png)

## 4. Implementation Steps

1. **CDC Ingestion:** Debezium connects to RDS PostgreSQL WAL, captures row-level transactional changes, and publishes them into the distributed 3-node Kafka cluster.
2. **Distributed Streaming to Raw/Bronze Layer:** The Spark Standalone Cluster (Master + 3 Workers) runs `HoodieMultiTableStreamer` to continuously ingest multi-table CDC events from Kafka into S3 as Hudi MOR tables.
3. **Staging/Silver Layer:** dbt (executed via Athena) reads raw Hudi tables, deduplicates records, handles tombstone deletes (`_hoodie_is_deleted`), and writes clean records into Apache Iceberg tables.
4. **Data Mart/Gold Layer:** dbt models the Silver data into the final Star Schema (central Facts and SCD Type 2 Dimensions) using Iceberg format, optimized for high-speed analytical scans.
5. **Orchestration with Airflow:** Apache Airflow triggers the dbt pipeline every 15 minutes (`dbt deps` -> `dbt run` -> `dbt test`) to maintain freshness with automated data quality gates.
6. **Analytics & BI Serving:** AWS Athena powers Metabase BI dashboards and the GenAI Text-to-SQL Agent (Google Gemini Pro + Next.js UI) for business reporting.

## 5. Performance Review

- **Storage & Query Optimization:** Transitioning from Hudi (Raw) to Iceberg (Staging/Mart) with Parquet columnar compression drastically reduced S3 storage footprint and accelerated Athena query speeds.
- **Partitioning:** Fact tables are strategically partitioned (e.g., by date), which minimizes the data scanned during Athena queries and directly reduces AWS costs.
- **Zero Source Impact:** The log-based CDC approach completely offloaded analytical workloads from the production PostgreSQL database, ensuring 100% transactional uptime.
- **Reliability & Data Quality:** Automated dbt tests integrated into the pipeline guarantee that no duplicate or malformed data propagates to executive dashboards.

## 6. Folder Structure

```text
dvdrental_streaming_data/
├── apps/                         # User-facing applications & microservices
│   ├── ai_agent/                 # GenAI backend service (FastAPI + Google Gemini)
│   └── ai_dashboard/             # Frontend analytics web app (Next.js + Tailwind CSS)
├── pipelines/                    # Data processing & lakehouse transformations
│   ├── dbt/                      # dbt transformation models (Silver/Staging & Gold/Mart)
│   ├── hudi/                     # Hudi Streamer ingestion configs (Kafka -> Hudi MOR)
│   └── airflow/                  # Orchestration DAGs, schedules & alerting plugins
├── infra/                        # Infrastructure as Code (IaC) & container environments
│   ├── docker/                   # Containerized local runtimes (Kafka, Debezium, Metabase)
│   │   ├── kafka/                # Zookeeper & Apache Kafka broker
│   │   ├── debezium/             # Debezium PostgreSQL CDC connector
│   │   └── metabase/             # Metabase BI container & dashboard exports
│   └── terraform_multi_node/     # AWS EC2 multi-node cluster provisioning (VPC, Kafka, Spark, RDS)
├── scripts/                      # Operational utilities & automation
│   ├── ops/                      # Lakehouse observability dashboard, offset/lag & WAL monitoring
│   ├── benchmark/                # Stress-testing & synthetic load generators
│   ├── setup/                    # WSL2 & local development bootstrap scripts
│   └── experiments/              # PySpark, DuckDB exploration & scratch scripts
└── docs/
    └── images/                   # Architecture diagrams & documentation assets
```

## 7. Data Serving & AI Dashboard

**Data Serving Architecture:**
The final Data Marts (Gold layer in Iceberg format) are queried directly from S3 via **AWS Athena** - a serverless interactive query engine. BI tools like Metabase (`infra/docker/metabase`) connect to Athena to render pre-defined analytics dashboards.

**AI Dashboard & Text-to-SQL Flow:**
To empower non-technical users, we built an innovative **AI Dashboard** that allows users to "chat with their data" in natural language:

1. **User Request:** A business user types a question (e.g., _"Show me the top 5 movie categories by revenue this month"_) into the Frontend Web App (`apps/ai_dashboard`).
2. **AI Agent (`apps/ai_agent`):** A backend Python service utilizing **Google Gemini Pro** processes the prompt. It understands the underlying Iceberg schema and generates an optimized, syntax-correct Athena SQL query.
3. **Execution & Rendering:** The Agent executes the generated SQL against AWS Athena, retrieves the results, and returns them to the frontend, which dynamically renders a custom chart or data table.

## 8. Business Intelligence Dashboards (Metabase)

A fully automated Metabase setup is provided via Python script (`infra/docker/metabase/setup_metabase.py`), which instantly provisions the following 4 interactive dashboards directly on top of the Iceberg Data Marts via AWS Athena.

### 1. Revenue & Payment Analytics

![Revenue Analytics 1](<infra/docker/metabase/dashboards/Metabase -  1. Revenue & Payment Analytics-images-0.jpg>)
![Revenue Analytics 2](<infra/docker/metabase/dashboards/Metabase -  1. Revenue & Payment Analytics-images-1.jpg>)

### 2. Rental Operations

![Rental Operations 1](<infra/docker/metabase/dashboards/Metabase -  2. Rental Operations_page-0001.jpg>)
![Rental Operations 2](<infra/docker/metabase/dashboards/Metabase -  2. Rental Operations_page-0002.jpg>)

### 3. Customer Intelligence

![Customer Intelligence](<infra/docker/metabase/dashboards/Metabase -  3. Customer Intelligence_page-0001.jpg>)

### 4. Film Catalog & Inventory

![Film Catalog 1](<infra/docker/metabase/dashboards/Metabase - 4. Film Catalog & Inventory_page-0001.jpg>)
![Film Catalog 2](<infra/docker/metabase/dashboards/Metabase - 4. Film Catalog & Inventory_page-0002.jpg>)

## 9. CI/CD Pipeline Automation (GitHub Actions)

The repository uses **GitHub Actions** for automated testing, dbt validation, and container build deployment:

- **Continuous Integration (`.github/workflows/ci.yml`)**:
  - **Code Quality**: Runs `ruff` Python linting/formatting checks, `pre-commit` hooks, and `shellcheck` for shell scripts.
  - **dbt Transformation CI**: Runs `dbt deps`, `dbt parse`, and `dbt compile` against mock profile configs to guarantee all SQL models, macros, and schema YAMLs compile without syntax errors.
  - **Airflow Pipeline CI**: Validates Python DAG compilation (`pipelines/airflow/dags/dbt_transform.py`) and Docker Compose configuration.

- **Continuous Deployment (`.github/workflows/cd.yml`)**:
  - **Airflow Container Build**: Builds and pushes production Docker images for Airflow (`pipelines/airflow`) to **GitHub Container Registry (GHCR)** on pushes to `main`/`master` or version tags (`v*`).
  - **DAG Artifact Packaging**: Bundles Airflow DAGs into release artifacts for automated deployment to production orchestration environments.

## 10. Data Quality & Integrity Engineering

Data quality in this lakehouse is not treated as a post-hoc cleanup step, but enforced as **continuous quality gates** across each tier of the Medallion architecture:

```text
┌─────────────────────────┐     ┌─────────────────────────┐     ┌─────────────────────────┐
│     CDC Source Gate     │ ──► │   Bronze / Hudi Gate    │ ──► │  Silver / Gold dbt Gate │
├─────────────────────────┤     ├─────────────────────────┤     ├─────────────────────────┤
│ • REPLICA IDENTITY FULL │     │ • Record Key Uniqueness │     │ • PK & Grain Validation │
│ • Dual-Envelope JSON    │     │ • Deterministic Merging │     │ • Referential Integrity │
│ • Schema Evolution      │     │ • Tombstone Filtering   │     │ • SCD Type 2 Intervals  │
└─────────────────────────┘     └─────────────────────────┘     └─────────────────────────┘
```

### 1. Ingestion & CDC Quality Gates
- **`REPLICA IDENTITY FULL` on PostgreSQL:** Guarantees that every UPDATE and DELETE transaction log emitted by Debezium contains both the full `before` and `after` row states. This prevents null join failures during downstream tombstone processing.
- **Deterministic Deduplication in Hudi:** Hudi's `DefaultHoodieRecordPayload` uses the Debezium event timestamp (`ts_ms`) and `_hoodie_commit_time` to deterministically resolve out-of-order Kafka message deliveries during Merge-On-Read compaction.

### 2. Transformation & Modeling Quality Gates (dbt Tests)
- **Granularity & Uniqueness:** Automated dbt assertions (`unique`, `not_null`) test every surrogate key (`rental_key`, `payment_key`, `customer_key`, `date_key`) to guarantee the primary grain is never compromised by duplicate CDC events.
- **Referential Integrity & Relationships:** Cross-model foreign key tests (`relationships`) ensure that 100% of transaction events in `fact_rental` and `fact_payment` resolve to valid records in `dim_customer`, `dim_film`, and `dim_store_staff`.
- **Semantic & Business Invariants:**
  - Positive monetary values: `amount >= 0.00`.
  - Temporal consistency: `rental_date <= return_date` (with null-safe handling for active rentals).
- **SCD Type 2 Correctness:**
  - **Strict Windowing:** `valid_from < valid_to` on all closed historical dimension rows.
  - **Single Active Version:** Exactly one active record (`is_current = true` and `valid_to IS NULL`) exists per natural business key (`customer_id`).

---

## 11. End-to-End Data Observability & Monitoring

The repository includes a dedicated, production-grade observability dashboard in [scripts/ops/monitor_pipeline.py](scripts/ops/monitor_pipeline.py) that monitors real-time streaming health across four operational pillars:

```bash
# Real-time multi-node observability dashboard with live auto-refresh (5s interval)
python3 scripts/ops/monitor_pipeline.py --watch 5

# Deep-dive inspection into specific CDC topic and consumer lag
python3 scripts/ops/monitor_pipeline.py --table rental
```

### 1. Kafka Stream & Consumer Lag Observability
- **Partition Offset Auditing:** Tracks Log Start Offset (LSO) and Log End Offset (LEO / High Watermark) across all **6 partitions** for each of the 15 CDC topics.
- **Consumer Group Lag:** Automatically computes per-partition consumer lag for Spark and Hudi consumer groups, pinpointing slow consumers or partition skew before it causes backlog spills.

### 2. Hudi Lakehouse Timeline & Freshness Metrics
- **Instant Timeline Inspection:** Connects directly to S3 via S3 VPC Gateway Endpoint to read `.hoodie/` timeline metadata.
- **Ingestion Lag Tracking:** Computes real-time data freshness by calculating the delta between the latest Hudi deltacommit timestamp and current UTC wall-clock time.
- **Compaction & Clean Auditing:** Monitors active table compaction states and cleaning metadata to ensure optimal Parquet file sizing.

### 3. Spark Cluster Health & Resource Tracking
- **Cluster Capacity:** Probes Spark Master REST API (`/json`) to report active worker counts (`ALIVE`/`DEAD`), assigned vs. available CPU cores, and allocated worker memory.
- **Streaming Job Status:** Continuously tracks streaming application states (`RUNNING`, `FINISHED`, `KILLED`) and driver event loops.

### 4. Operational Alerting & Failure Handling
- **Slack Webhook Integration:** Apache Airflow tasks are bound to failure callbacks that automatically deliver structured Slack alerts containing DAG IDs, task run timestamps, exception traces, and direct links to Airflow logs.

---

## 12. Defensive Python & Resilient Engineering Practices

All Python services in the repository (including the high-throughput CDC generator [producer.py](infra/terraform_multi_node/producer/producer.py) and monitoring suite [monitor_pipeline.py](scripts/ops/monitor_pipeline.py)) strictly adhere to **Defensive Programming Principles**:

### 1. Fast Timeouts & Non-Blocking Network Probes
- **TCP Reachability Probes:** Before invoking heavyweight SDK clients (Boto3, KafkaConsumer), lightweight socket checks (`socket.create_connection((host, port), timeout=0.5)`) verify network path accessibility.
- **Strict Boto3 Timeouts:** AWS API clients are explicitly initialized with bounded connect/read timeouts to prevent threads from hanging indefinitely during AWS network degradation:
  ```python
  cfg = Config(connect_timeout=3, read_timeout=3, retries={"max_attempts": 1})
  s3 = boto3.client("s3", region_name=region_name, config=cfg)
  ```

### 2. Bounded Memory Footprint (<50MB RAM)
- **Ring-Buffer Pools:** The synthetic generator uses fixed-size deques (`collections.deque(maxlen=100_000)`) to maintain active unreturned rentals. This guarantees a flat, deterministic memory footprint (<50MB RAM) even when streaming **5,000,000+ CDC transactions**.

### 3. Multi-Tier Dynamic Discovery with Graceful Fallbacks
Services dynamically discover broker endpoints and cluster topology through a resilient fallback hierarchy that prevents crashes across diverse development and cloud environments:
$$\text{CLI Flags} \longrightarrow \text{Environment Variables} \longrightarrow \text{AWS Secrets Manager} \longrightarrow \text{EC2 Tag Discovery} \longrightarrow \text{Static VPC IP Fallback}$$

### 4. Stateful Idempotency & Checkpointing
- **Zero ID Collisions:** The generator maintains state persistence across runs via a local state file (`~/.dvdrental_producer_state.json`) backed up to S3 (`s3://${BUCKET}/metadata/producer_state.json`).
- Subsequent streaming runs resume from the exact high-watermark IDs (`last_rental_id`, `last_payment_id`), ensuring 100% idempotent data generation without primary key collision.

### 5. Explicit Exception Hierarchies
- **No Broad `except:` Swallowing:** Code catches precise exceptions (`botocore.exceptions.ClientError`, `KafkaError`, `urllib.error.URLError`, `json.JSONDecodeError`). Unrecoverable corruptions fail fast and log actionable diagnostic context rather than silently continuing with bad data.

### 6. Safe Buffer Flushing & Resource Cleanup
- Production batching configurations (`linger_ms=20`, `batch_size=65536`, `compression_type='lz4'`) are guarded with explicit `producer.flush()` and `producer.close()` calls inside `try...finally` blocks to guarantee zero message loss upon SIGINT/SIGTERM signals.
