---
name: de-task
description: Runs a controlled data engineering task lifecycle: inspect the repository, investigate a bug or feature, create an implementation plan, wait for approval, then implement and verify. Use for non-trivial pipeline, SQL, schema, orchestration, or platform changes.
---

# DE Task Lifecycle

## Phase 0 — Protect current work
1. Inspect `git status --short` before changing anything.
2. Read README/project docs and identify actual languages, frameworks, storage systems, orchestration, data contracts, and test commands.
3. Preserve pre-existing modifications. Do not reset or clean the working tree.

## Phase 1 — Explore only
Do not edit files in this phase.
1. Restate the requested and expected behavior.
2. Trace the relevant flow from source to transformation to sink/consumer.
3. Inspect only relevant files, configs, models, and tests; cite file paths and symbols as evidence.
4. Separate confirmed facts from hypotheses.
5. Identify data correctness risks: grain, keys, joins, nulls, duplicates, ordering, time zones, schema evolution, late data, reruns, and permissions as relevant.
6. Propose the smallest safe approach, affected files, alternatives/trade-offs, tests, rollout/backfill considerations, and explicit non-goals.
7. Return a concise plan and STOP. Ask for explicit approval before editing.

## Phase 2 — Implement only after approval
1. Recheck `git status` and the approved plan.
2. Implement in small, reviewable increments. Do not expand scope silently.
3. Add/update tests for the changed behavior.
4. If the repository reveals a material contradiction or the plan needs a breaking/schema/infra change, stop and ask rather than improvising.

## Phase 3 — Verify
1. Discover and run relevant existing checks; do not guess command names.
2. Check correctness beyond happy paths: empty input, nulls, duplicates, boundary timestamps, retries/reruns, late records, schema mismatch, and access boundaries where relevant.
3. Inspect the actual diff for accidental changes, secrets, generated files, and unrelated edits.
4. Report commands actually run and observed outcomes; list checks not run and why.
5. Summarize changed files, choices, residual risks, and rollback/backfill notes if relevant.

## Required output for Phase 1
- Requirement / acceptance criteria
- Current data flow and evidence
- Confirmed facts vs hypotheses
- Candidate root cause or design options
- Minimal proposed plan by file/component
- Tests and data-quality checks
- Risks, assumptions, out-of-scope items
- Explicit statement: no files were modified
