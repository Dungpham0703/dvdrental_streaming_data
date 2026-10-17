---
name: data-quality
description: Designs data quality validation, monitoring, reconciliation, freshness checks, anomaly detection, and failure handling for batch or streaming pipelines. Use when data can be complete but wrong, or when defining quality gates and observability.
---

# Data Quality Engineering

## Design checks from an invariant
For each check, state:
- Business/data invariant
- Scope and grain
- Measurement/query
- Threshold or expected range, including whether it is a temporary assumption
- Severity and owner/action on failure
- Whether the check blocks the pipeline, quarantines rows, or only alerts

## Check dimensions as relevant
- Schema/contract: expected columns, types, required fields, allowed values
- Completeness: nulls/missing partitions/expected entities
- Uniqueness: key uniqueness and duplicate behavior
- Validity: range, format, temporal/business rules
- Referential integrity: key relationships and orphan records
- Consistency/reconciliation: source-to-target counts, totals, hashes, balances, or sampled comparisons
- Freshness/timeliness: maximum data age and late-arrival distribution
- Distribution/anomalies: volume shifts, unexpected category changes, numeric drift

## Implementation guidance
1. Inspect current data contracts, tests, monitoring, and business expectations before inventing rules.
2. Do not equate `NOT NULL`/`UNIQUE` with correctness. Include business rules and cross-table consistency where valuable.
3. Use robust denominators and define handling for empty datasets, late data, seasonality, and small samples.
4. Use statistical thresholds only with an explainable baseline and enough history; avoid treating every normal fluctuation as a failure.
5. Define how failed/invalid rows are retained, quarantined, replayed, or corrected. Avoid silent drops.
6. Keep checks observable: report counts/rates, affected partitions/windows, and run identifiers without exposing sensitive records.
7. Test both passing and failing fixtures, including boundaries. Demonstrate that the check actually catches the targeted bad data.
8. Do not invent thresholds. Mark them as configurable proposals until confirmed by requirements or historical evidence.
