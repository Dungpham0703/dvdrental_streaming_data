---
name: python-data-pipeline
description: Builds or reviews robust Python ETL/ELT jobs, ingestion scripts, API/file extractors, and scheduled data pipelines. Use for retry logic, logging, incremental loads, idempotency, schemas, and operational reliability.
---

# Python Data Pipeline Engineering

1. Inspect the project's Python version, dependency manager, formatting/linting, types, test framework, and current patterns first.
2. Separate extraction, validation, transformation, and loading where doing so improves testability; avoid unnecessary architecture for tiny scripts.
3. Make configuration explicit and secrets external. Never hardcode credentials or log tokens, raw PII, or full sensitive payloads.
4. Use structured logging with useful context (job/run ID, source, batch/window, counts, duration), while avoiding secrets and sensitive row contents. Do not replace operational logging with unstructured `print` calls.
5. Handle errors deliberately. Retry only errors likely to be transient, use bounded retries/backoff, and make timeouts explicit. Do not swallow broad exceptions or log-and-continue after data corruption.
6. Design writes to be idempotent where possible. Explain behavior after partial failure, retry, duplicate input, rerun, and backfill.
7. For incremental ingestion, document cursor/watermark semantics, overlap windows, late updates, deletion handling, and checkpoint advancement. Advance a watermark only when the corresponding data is safely persisted according to the design.
8. Validate schemas and required fields at boundaries. Be explicit about encoding, timestamps/time zones, numeric conversion, missing values, and malformed records.
9. Keep transformations deterministic and unit-testable. Use dependency injection/mocks for external APIs and storage when helpful.
10. Test failure paths, empty input, duplicate batches, partial writes, and retry idempotency—not only a successful run.
11. Discover actual project commands before running checks; report which were executed and their results.
