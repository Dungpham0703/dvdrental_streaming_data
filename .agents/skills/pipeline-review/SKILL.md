---
name: pipeline-review
description: Performs an independent, read-only review of a Git diff or implemented data engineering change for correctness, data loss, duplicates, security, reliability, and performance regressions. Use after implementation or before merging a pipeline/model change.
---

# Independent Pipeline Review

## Rules
- Read-only: do not edit files or auto-fix findings during the first review.
- Inspect the task requirements, approved plan, actual diff, relevant surrounding code, configs, and tests.
- Do not assume the implementer's summary is complete. Verify claims from code and executed test output.
- Do not invent a bug; describe a concrete failure scenario and evidence for every finding.

## Review dimensions
1. Requirement/contract: does the diff implement acceptance criteria without unrelated scope?
2. Data correctness: grain changes, fanout joins, null semantics, deduplication ordering, filtering, time zones, numeric precision, deletes/updates.
3. Reliability: retries, timeouts, partial failure, idempotency, transaction boundaries, checkpoint/watermark advancement, backfills and reruns.
4. Quality gates: do tests check the intended failure, or only the happy path? Are uniqueness, completeness, freshness, relationships, and reconciliation needed?
5. Security/privacy: secrets, SQL injection, access control, unsafe logs, sensitive data exposure.
6. Performance/cost: unnecessary scans, shuffles, `collect()`, unbounded memory, inefficient joins, small-file explosion, or accidental full reloads.
7. Operability: useful logs/metrics, actionable error messages, recovery instructions, and safe migration/rollback path.

## Output format
- Summary: approve / approve with follow-up / changes requested (recommendation, not a merge action)
- Findings ordered by severity: Critical / High / Medium / Low
- For each finding: file/line, concrete scenario, impact, evidence, minimal fix, and test to prove it
- Tests/results verified; checks not run
- Residual risks and aspects that cannot be confirmed from the diff

If no findings are identified, state what was reviewed and the limits of the review. Never imply that no issues exist merely because tests passed.
