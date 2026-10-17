---
name: pyspark-engineering
description: Implements, explains, debugs, and optimizes PySpark DataFrame and Structured Streaming jobs. Use for lazy evaluation, query plans, joins, shuffles, skew, partitioning, caching, file formats, or Spark tests.
---

# PySpark Engineering

1. Inspect the Spark/PySpark version and deployment environment. Do not assume managed Databricks or a specific cloud runtime unless the repo proves it.
2. Explain laziness accurately: transformations build a plan; actions trigger execution. Identify actions that may trigger repeated jobs.
3. Prefer built-in Spark SQL/DataFrame expressions over Python UDFs when possible so Spark can optimize more of the plan.
4. Inspect `explain()`/physical plans when useful. Identify wide transformations, shuffle boundaries, sort/window operations, skewed keys, and join-size risks.
5. Do not recommend `repartition`, `coalesce`, broadcast joins, or caching generically. State the reason and validate against data size, key distribution, and execution plan.
6. Avoid `collect()` or conversion to Pandas for datasets that may exceed driver memory. Use bounded samples for inspection and do not print raw sensitive rows.
7. For Parquet/data-lake output, consider file size, partition cardinality, predicate pushdown, schema evolution, and small-file risk. Do not partition by very high-cardinality keys by default.
8. Preserve semantics during optimization: null behavior, duplicate handling, timestamp/time-zone semantics, ordering, and aggregation grain.
9. For Structured Streaming, check output mode, checkpoint location, stateful operations, event-time/watermarks, late events, restart behavior, and sink guarantees.
10. Test transformations on small representative fixtures plus edge cases; separate logic tests from cluster-scale performance claims.
11. Never claim a speedup without benchmark or plan evidence. State what was measured and under which environment/data shape.
