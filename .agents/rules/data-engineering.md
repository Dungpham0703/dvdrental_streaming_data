---
trigger: glob
description: Apply these principles when writing or reviewing SQL, ETL/ELT pipelines, data models, data quality checks, PySpark jobs, streaming or CDC systems.
globs: **/*.sql, **/*.py, **/*.yml, **/*.tf, **/*.properties
---

# Data Engineering Principles

- Before transformations, define the intended output grain and key. Check whether joins can multiply rows and whether aggregations preserve the intended grain.
- Make incremental processing rerunnable. Explain the idempotency strategy, watermark semantics, late-arriving data behavior, and backfill path when applicable.
- Prefer explicit schema/columns and deterministic transformations. Avoid `SELECT *` in long-lived models or public data contracts unless the repository has a clear reason.
- Treat null semantics, timezone handling, numeric precision, deduplication ordering, and schema evolution as correctness concerns—not cleanup details.
- For data-quality checks, state the invariant, scope, threshold, severity, and expected response to a failure. Do not invent thresholds without documenting them as configurable assumptions.
- For Spark, consider execution plans, shuffle boundaries, skew, partition count, file sizes, and whether caching is justified by reuse. Do not optimize based only on code appearance.
- For streaming/CDC, distinguish source delivery semantics from sink guarantees. Do not casually promise end-to-end exactly-once behavior.
- Verify assumptions from project code/configuration and official documentation when version-specific behavior matters.

# Execution & Terminal Guardrails

- NEVER execute `terraform destroy`, `terraform apply -auto-approve`, or `docker system prune -a` without explicit human confirmation.
- NEVER run `aws s3 rm --recursive` or delete Hudi metadata (`.hoodie` directory) on target buckets.
- For dbt changes in `pipelines/dbt/`, always run `dbt compile` or `dbt run --select <model_name> --empty` (dry-run) to verify schema/lineage before full execution.
- Do not modify `.env`, `.aws/`, or credentials files directly.
