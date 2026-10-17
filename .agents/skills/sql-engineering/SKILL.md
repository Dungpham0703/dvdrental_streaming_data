---
name: sql-engineering
description: Writes, debugs, reviews, and optimizes analytical SQL and data models with attention to grain, join cardinality, null semantics, incremental logic, correctness, and query cost. Use for SQL transformations, warehouse queries, or schema/model review.
---

# SQL Engineering

1. Identify the SQL dialect and engine from the repo (e.g., Postgres, BigQuery, Spark SQL, DuckDB); do not assume.
2. State the input and output grain. Identify keys and expected row-count behavior before changing joins or aggregations.
3. Check join cardinality and row multiplication. Use small counterexample queries or fixtures where helpful.
4. Make column selection explicit. Use clear aliases and readable CTEs; avoid hiding complex logic in deeply nested expressions.
5. Reason explicitly about `NULL`, `COUNT(*)` vs `COUNT(column)`, `NOT IN` with nulls, duplicate keys, timestamp boundaries, time zones, and decimal precision.
6. For deduplication, define the deterministic winner ordering; never assume arbitrary row order is stable.
7. For incremental models, define the watermark/key strategy, late-arriving data behavior, updates/deletes, reruns, and backfills.
8. For performance, inspect partition filters, predicate pushdown, join strategy, data scanned, sort/window costs, and explain plans if the engine supports them. Avoid claiming performance improvement without evidence.
9. Preserve data contracts. Do not silently rename/drop/change columns or alter nullability.
10. Add dialect-appropriate tests/checks when available: uniqueness, not-null, accepted values, relationships, reconciliation, and row-count expectations.

## Output when reviewing SQL
- Correctness findings first, with file/line/query evidence
- Grain and cardinality analysis
- Null/edge-case behavior
- Performance/cost concerns with their conditions
- Suggested minimal change and tests
