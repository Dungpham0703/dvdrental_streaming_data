# Real-Time E-Commerce Analytics Lakehouse (CDC & Medallion Architecture)

> Architected & Implemented by: Dung Pham  
> Tech Stack: AWS RDS Postgres, Debezium, Apache Kafka, Apache Hudi, AWS S3, dbt, Apache Athena/Spark, Docker, GitHub Actions CI/CD

---

## 1. Business Problem & Requirements

### Context & Business Challenge

In a transactional system (DVD Rental / E-Commerce), operational data (rentals, payments, customer details) changes continuously in real-time. Traditional OLTP systems (PostgreSQL) face several analytical challenges:

- High System Load: Running complex analytical queries directly on the OLTP database severely degrades the performance of end-user transactions.
- Loss of Historical Data: When customer information (address, email, store_id) is updated, the old data is overwritten, making it impossible to analyze past behaviors accurately.
- High Latency in Reporting: Traditional nightly batch ETL pipelines cannot meet the business demand for near real-time tracking of revenue, rental trends, and inventory status.

### Requirements & Goals

1. CDC Real-Time Ingestion: Automatically capture all INSERT, UPDATE, and DELETE events from PostgreSQL without impacting the OLTP database performance.
2. Medallion Architecture on Data Lake: Build a 3-layer data pipeline (Bronze, Silver, Gold) stored on AWS S3.
3. Slowly Changing Dimensions (SCD Type 2): Preserve the complete history of customer profile changes to enable accurate Customer 360 analysis.
4. Decoupled & Automated Infrastructure: Deploy a distributed ingestion cluster on AWS EC2 using Docker Compose, managed and automated via GitHub Actions CI/CD.

---

## 2. Data Understanding & Data Modeling

### Source Dataset (DVD Rental Operational DB)

The dataset consists of 15 normalized (3NF) tables on PostgreSQL RDS, representing a real-world operational system: customer, rental, payment, film, actor, inventory, store, staff, address, city, country, etc.

### Data Warehouse Modeling (Star Schema & SCD Type 2)

To transform the data from 3NF into an analytics-ready format, the system is modeled using a Star Schema:

- Fact Tables (Transactional Events):
  - fact_rental: Records individual rental transactions (rental_id, customer_id, film_id, staff_id, rental_date, return_date).
  - fact_payment: Records financial transactions (payment_id, customer_id, staff_id, amount, payment_date).
- Dimension Tables (Descriptive Attributes):
  - dim_customer (SCD Type 2): Utilizes valid_from, valid_to, and is_current fields to track historical changes in customer profiles.
  - dim_film: Detailed film attributes (title, category, rating, rental_rate, length).
  - dim_store_staff: Store, staff, and geographical hierarchy (address, city, country).

---

## 3. Tech Stack & System Architecture

### Technology Stack

- Database Source: AWS RDS PostgreSQL (Configured with rds.logical_replication = 1 / wal_level = logical).
- CDC Engine: Debezium Connect (Kafka Connect).
- Event Streaming Broker: Apache Kafka & Zookeeper.
- Storage Format & Lakehouse Engine: Apache Hudi (Merge-On-Read - MOR mode).
- Data Lake Storage: AWS S3 (Bronze / Silver / Gold layers).
- Transformation & Query Engine: dbt (data build tool), AWS Athena / Apache Spark.
- DevOps & Infrastructure: AWS EC2, Docker Compose, Linux Bash, Git, GitHub Actions CI/CD.

### Overall Architecture (Modern Medallion Lakehouse)

[AWS RDS Postgres Source]
| (WAL Log / pgoutput)
v
[Debezium Connect Node - EC2 Node 2]
| (Topic: cdc_postgres_rds.public.\*)
v
[Kafka Broker Node - EC2 Node 1]
| (Hudi MultiTableStreamer / Spark)
v
[BRONZE LAYER] (Raw S3 Data Lake - Parquet/Avro)
| (dbt / Athena Transformation)
v
[SILVER LAYER] (Cleansed / Staging Data - S3)
| (dbt Transformation - Star Schema & SCD Type 2)
v
[GOLD LAYER] (Data Marts & Aggregations - S3)
|
v
[BI Dashboard / Metabase / Analytics]

---

## 4. Implementation Details

### 4.1. Database Configuration (AWS RDS PostgreSQL)

- Configured Parameter Group: rds.logical_replication = 1.
- Loaded the 15 standard dvdrental tables.
- Established VPC Security Groups to isolate internal traffic between RDS and the EC2 cluster.

### 4.2. Multi-Node Distributed Ingestion Setup (AWS EC2 & Docker)

The ingestion system decouples storage and compute resources:

- Node 1 (Kafka Broker - EC2 8GB RAM): Hosts Zookeeper (Port 2181) and Kafka Broker (Port 9092).
- Node 2 (Debezium Connect - EC2 8GB RAM): Hosts Debezium Connect 2.4 (Port 8083), pointing to Node 1's Private IP.

### 4.3. CDC Connector Registration

Triggered the Debezium Connector via REST API to continuously monitor the 15 operational tables:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @postgres_source.json \
  http://<EC2_CONNECT_PRIVATE_IP>:8083/connectors
```
