"""Automated regression test suite for CDC DELETE propagation across Silver and Gold layers.

Validates:
1. Model configuration and SQL structure in stg_rental, stg_payment, fct_rental, fct_payment,
   fct_rental_active, fct_payment_active, and fct_daily_revenue for soft delete semantics.
2. Functional execution of CDC event lifecycle (INSERT, UPDATE, SOFT DELETE, REPLAY, RECREATE,
   NULL-PRESERVATION, ACTIVE VIEW FILTERING, REVENUE ISOLATION).
"""

from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path


class TestCdcSoftDeleteModelStructure(unittest.TestCase):
    """Verify that dbt model files contain the required soft delete configs, ordering guards, and views."""

    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.rental_model = (
            self.repo_root / "pipelines/dbt/models/staging/stg_rental.sql"
        )
        self.payment_model = (
            self.repo_root / "pipelines/dbt/models/staging/stg_payment.sql"
        )
        self.fct_rental_model = (
            self.repo_root / "pipelines/dbt/models/mart/fct_rental.sql"
        )
        self.fct_payment_model = (
            self.repo_root / "pipelines/dbt/models/mart/fct_payment.sql"
        )
        self.fct_rental_active = (
            self.repo_root / "pipelines/dbt/models/mart/fct_rental_active.sql"
        )
        self.fct_payment_active = (
            self.repo_root / "pipelines/dbt/models/mart/fct_payment_active.sql"
        )
        self.fct_daily_revenue = (
            self.repo_root / "pipelines/dbt/models/mart/fct_daily_revenue.sql"
        )
        self.int_summary_model = (
            self.repo_root
            / "pipelines/dbt/models/intermediate/int_customer_rental_summary.sql"
        )

    def test_stg_rental_has_soft_delete_config(self) -> None:
        content = self.rental_model.read_text(encoding="utf-8")
        self.assertIn("insert_condition='(src.is_deleted = false)'", content)
        self.assertIn("update_condition=", content)
        self.assertIn("src.cdc_capture_at > target.cdc_capture_at", content)
        self.assertIn("merge_update_columns_rules=", content)
        self.assertIn("__op = 'd'", content)
        self.assertIn("is_deleted desc", content)

    def test_stg_payment_has_soft_delete_config(self) -> None:
        content = self.payment_model.read_text(encoding="utf-8")
        self.assertIn("insert_condition='(src.is_deleted = false)'", content)
        self.assertIn("update_condition=", content)
        self.assertIn("src.cdc_capture_at > target.cdc_capture_at", content)
        self.assertIn("merge_update_columns_rules=", content)
        self.assertIn("__op = 'd'", content)
        self.assertIn("is_deleted desc", content)

    def test_fct_rental_retains_soft_deletes_with_ordering(self) -> None:
        content = self.fct_rental_model.read_text(encoding="utf-8")
        self.assertIn("insert_condition='(src.is_deleted = false)'", content)
        self.assertIn("update_condition=", content)
        self.assertIn("merge_update_columns_rules=", content)
        self.assertIn("r.is_deleted", content)
        self.assertIn(
            "'customer_key': 'case when src.is_deleted then _old_ else _new_ end'",
            content,
        )
        self.assertIn("coalesce(c.customer_key, '-1') as customer_key", content)
        # Must not physically delete rows
        self.assertNotIn("delete_condition", content)

    def test_fct_payment_retains_soft_deletes_with_ordering(self) -> None:
        content = self.fct_payment_model.read_text(encoding="utf-8")
        self.assertIn("insert_condition='(src.is_deleted = false)'", content)
        self.assertIn("update_condition=", content)
        self.assertIn("merge_update_columns_rules=", content)
        self.assertIn("p.is_deleted", content)
        self.assertIn(
            "'customer_key': 'case when src.is_deleted then _old_ else _new_ end'",
            content,
        )
        self.assertIn("coalesce(c.customer_key, '-1') as customer_key", content)
        # Must not physically delete rows
        self.assertNotIn("delete_condition", content)

    def test_active_views_exist_and_filter_soft_deleted(self) -> None:
        rental_active = self.fct_rental_active.read_text(encoding="utf-8")
        payment_active = self.fct_payment_active.read_text(encoding="utf-8")
        self.assertIn("where is_deleted = false", rental_active)
        self.assertIn("where is_deleted = false", payment_active)

    def test_fct_daily_revenue_uses_active_payments(self) -> None:
        content = self.fct_daily_revenue.read_text(encoding="utf-8")
        self.assertIn("fct_payment_active", content)

    def test_int_customer_rental_summary_filters_soft_deleted(self) -> None:
        content = self.int_summary_model.read_text(encoding="utf-8")
        self.assertIn("where is_deleted = false", content)

    def test_staging_dimensions_have_soft_delete_and_ordering(self) -> None:
        for dim_file in [
            "stg_film.sql",
            "stg_customer.sql",
            "stg_actor.sql",
            "stg_address.sql",
            "stg_staff.sql",
            "stg_store.sql",
        ]:
            model_path = self.repo_root / f"pipelines/dbt/models/staging/{dim_file}"
            content = model_path.read_text(encoding="utf-8")
            self.assertIn(
                "is_deleted", content, f"{dim_file} missing is_deleted column"
            )
            self.assertIn(
                "order by cdc_capture_at desc, is_deleted desc",
                content,
                f"{dim_file} missing is_deleted tie-breaker",
            )
            self.assertNotIn(
                "where __deleted = false",
                content,
                f"{dim_file} should not filter before deduplication",
            )

    def test_metabase_setup_uses_active_views_and_filters_deleted(self) -> None:
        metabase_file = self.repo_root / "infra/docker/metabase/setup_metabase.py"
        if not metabase_file.exists():
            self.skipTest("infra/docker/metabase/setup_metabase.py does not exist")
        content = metabase_file.read_text(encoding="utf-8")
        self.assertNotIn("dvdrental_mart.fct_payment ", content)
        self.assertNotIn("dvdrental_mart.fct_rental ", content)
        self.assertIn("dvdrental_mart.fct_payment_active", content)
        self.assertIn("dvdrental_mart.fct_rental_active", content)
        self.assertIn("is_deleted = false", content)

    def test_ai_agent_semantic_dictionary_includes_soft_delete(self) -> None:
        semantic_file = self.repo_root / "apps/ai_agent/semantic_dictionary.json"
        import json

        with open(semantic_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        for tbl_name, tbl_info in data["tables"].items():
            self.assertIn(
                "is_deleted",
                tbl_info["columns"],
                f"{tbl_name} missing is_deleted column",
            )
        join_guide_str = json.dumps(data["join_guide"])
        self.assertIn("fct_payment_active", join_guide_str)
        self.assertIn("fct_rental_active", join_guide_str)
        self.assertIn("is_deleted = false", join_guide_str)


class TestCdcSoftDeleteMergeLifecycleSimulation(unittest.TestCase):
    """Simulate SQLite-based state transitions for Iceberg MERGE soft delete contract.

    Tests scenarios T-01 through T-09:
    - T-01: INSERT -> DELETE preserves target with is_deleted=1 and retains historical payload
    - T-02: DELETE -> late UPDATE attempt is rejected by update_condition (no resurrection)
    - T-03: Same-timestamp UPDATE + DELETE resolves with DELETE winning
    - T-04: Legitimate UPDATE setting a column to NULL correctly sets it to NULL
    - T-05: DELETE for a non-existent key is not inserted (no ghost row)
    - T-06: DELETE -> RECREATE with strictly newer timestamp reactivates record
    - T-07: Facts retain deleted records with is_deleted=1
    - T-08: Active views filter out soft-deleted records
    - T-09: Revenue calculation only aggregates active payments
    """

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()

        # Target Silver table representing Iceberg table state
        self.cur.execute("""
            CREATE TABLE target_stg_rental (
                rental_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                inventory_id INTEGER,
                staff_id INTEGER,
                rental_date TEXT,
                return_date TEXT,
                last_update TEXT,
                cdc_capture_at TEXT,
                is_deleted INTEGER NOT NULL DEFAULT 0
            )
        """)

        # Target Gold fact payment table
        self.cur.execute("""
            CREATE TABLE target_fct_payment (
                payment_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                staff_id INTEGER,
                rental_id INTEGER,
                amount REAL,
                payment_date TEXT,
                cdc_capture_at TEXT,
                is_deleted INTEGER NOT NULL DEFAULT 0
            )
        """)
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def _execute_merge(self, incoming_events: list[dict]) -> None:
        """Simulate deduplication CTE + Iceberg MERGE statement execution with update_condition."""
        self.cur.execute(
            "CREATE TEMP TABLE IF NOT EXISTS tmp_incoming ("
            "rental_id INT, customer_id INT, inventory_id INT, staff_id INT, "
            "rental_date TEXT, return_date TEXT, last_update TEXT, "
            "cdc_capture_at TEXT, __op TEXT, __deleted TEXT)"
        )
        self.cur.execute("DELETE FROM tmp_incoming")

        for ev in incoming_events:
            self.cur.execute(
                "INSERT INTO tmp_incoming VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ev["rental_id"],
                    ev.get("customer_id"),
                    ev.get("inventory_id"),
                    ev.get("staff_id"),
                    ev.get("rental_date"),
                    ev.get("return_date"),
                    ev.get("last_update"),
                    ev["cdc_capture_at"],
                    ev.get("__op", "u"),
                    ev.get("__deleted", "false"),
                ),
            )

        # Staging deduplication CTE with intra-batch tie-breaker: is_deleted DESC
        self.cur.execute("""
            CREATE TEMP TABLE tmp_src AS
            WITH selected AS (
                SELECT
                    rental_id,
                    customer_id,
                    inventory_id,
                    staff_id,
                    rental_date,
                    return_date,
                    last_update,
                    cdc_capture_at,
                    CASE
                        WHEN __op = 'd' THEN 1
                        WHEN LOWER(TRIM(COALESCE(__deleted, 'false'))) IN ('true', 't', '1') THEN 1
                        ELSE 0
                    END AS is_deleted
                FROM tmp_incoming
            ),
            deduplicated AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY rental_id
                        ORDER BY cdc_capture_at DESC, is_deleted DESC
                    ) AS dup_rank
                FROM selected
            )
            SELECT rental_id, customer_id, inventory_id, staff_id, rental_date, return_date, last_update, cdc_capture_at, is_deleted
            FROM deduplicated
            WHERE dup_rank = 1
        """)

        # MERGE Step 1: WHEN MATCHED AND update_condition THEN UPDATE
        # update_condition: src.cdc_capture_at > target.cdc_capture_at OR (src.cdc_capture_at = target.cdc_capture_at AND src.is_deleted = 1)
        # Selective payload preservation: CASE WHEN src.is_deleted THEN target.col ELSE src.col END
        self.cur.execute("""
            UPDATE target_stg_rental
            SET customer_id = (
                    SELECT CASE WHEN s.is_deleted = 1 THEN target_stg_rental.customer_id ELSE s.customer_id END
                    FROM tmp_src s WHERE s.rental_id = target_stg_rental.rental_id
                ),
                inventory_id = (
                    SELECT CASE WHEN s.is_deleted = 1 THEN target_stg_rental.inventory_id ELSE s.inventory_id END
                    FROM tmp_src s WHERE s.rental_id = target_stg_rental.rental_id
                ),
                staff_id = (
                    SELECT CASE WHEN s.is_deleted = 1 THEN target_stg_rental.staff_id ELSE s.staff_id END
                    FROM tmp_src s WHERE s.rental_id = target_stg_rental.rental_id
                ),
                rental_date = (
                    SELECT CASE WHEN s.is_deleted = 1 THEN target_stg_rental.rental_date ELSE s.rental_date END
                    FROM tmp_src s WHERE s.rental_id = target_stg_rental.rental_id
                ),
                return_date = (
                    SELECT CASE WHEN s.is_deleted = 1 THEN target_stg_rental.return_date ELSE s.return_date END
                    FROM tmp_src s WHERE s.rental_id = target_stg_rental.rental_id
                ),
                last_update = (
                    SELECT CASE WHEN s.is_deleted = 1 THEN target_stg_rental.last_update ELSE s.last_update END
                    FROM tmp_src s WHERE s.rental_id = target_stg_rental.rental_id
                ),
                cdc_capture_at = (
                    SELECT s.cdc_capture_at FROM tmp_src s WHERE s.rental_id = target_stg_rental.rental_id
                ),
                is_deleted = (
                    SELECT s.is_deleted FROM tmp_src s WHERE s.rental_id = target_stg_rental.rental_id
                )
            WHERE rental_id IN (
                SELECT s.rental_id FROM tmp_src s
                WHERE s.rental_id = target_stg_rental.rental_id
                  AND (
                      s.cdc_capture_at > target_stg_rental.cdc_capture_at
                      OR (s.cdc_capture_at = target_stg_rental.cdc_capture_at AND s.is_deleted = 1)
                  )
            )
        """)

        # MERGE Step 2: WHEN NOT MATCHED AND (src.is_deleted = false) THEN INSERT
        self.cur.execute("""
            INSERT INTO target_stg_rental
            SELECT rental_id, customer_id, inventory_id, staff_id, rental_date, return_date, last_update, cdc_capture_at, is_deleted
            FROM tmp_src s
            WHERE s.is_deleted = 0
              AND s.rental_id NOT IN (SELECT rental_id FROM target_stg_rental)
        """)

        self.cur.execute("DROP TABLE tmp_src")
        self.conn.commit()

    def test_t01_insert_then_delete_preserves_historical_payload(self) -> None:
        # INSERT
        self._execute_merge(
            [
                {
                    "rental_id": 101,
                    "customer_id": 42,
                    "inventory_id": 5,
                    "staff_id": 2,
                    "rental_date": "2026-10-18 01:00:00.000",
                    "return_date": "2026-10-18 02:00:00.000",
                    "last_update": "2026-10-18 01:00:00.000",
                    "cdc_capture_at": "2026-10-18 01:00:00.000",
                    "__op": "c",
                    "__deleted": "false",
                }
            ]
        )

        # DELETE with NULL non-key columns (typical PG WAL payload)
        self._execute_merge(
            [
                {
                    "rental_id": 101,
                    "customer_id": None,
                    "inventory_id": None,
                    "staff_id": None,
                    "rental_date": None,
                    "return_date": None,
                    "last_update": None,
                    "cdc_capture_at": "2026-10-18 02:30:00.000",
                    "__op": "d",
                    "__deleted": "true",
                }
            ]
        )

        self.cur.execute("SELECT * FROM target_stg_rental WHERE rental_id = 101")
        row = self.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["is_deleted"], 1)
        # Historical attributes must be preserved
        self.assertEqual(row["customer_id"], 42)
        self.assertEqual(row["inventory_id"], 5)
        self.assertEqual(row["rental_date"], "2026-10-18 01:00:00.000")

    def test_t02_delete_then_late_update_resurrection_prevented(self) -> None:
        self.test_t01_insert_then_delete_preserves_historical_payload()

        # Late replayed older UPDATE from 01:30 (before DELETE at 02:30)
        self._execute_merge(
            [
                {
                    "rental_id": 101,
                    "customer_id": 42,
                    "inventory_id": 5,
                    "staff_id": 2,
                    "rental_date": "2026-10-18 01:00:00.000",
                    "return_date": "2026-10-18 01:30:00.000",
                    "last_update": "2026-10-18 01:30:00.000",
                    "cdc_capture_at": "2026-10-18 01:30:00.000",
                    "__op": "u",
                    "__deleted": "false",
                }
            ]
        )

        self.cur.execute("SELECT * FROM target_stg_rental WHERE rental_id = 101")
        row = self.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(
            row["is_deleted"], 1, "Must NOT be resurrected by older update"
        )
        self.assertEqual(row["cdc_capture_at"], "2026-10-18 02:30:00.000")

    def test_t03_same_timestamp_update_and_delete_in_batch(self) -> None:
        # Both arrive with identical cdc_capture_at
        same_ts_events = [
            {
                "rental_id": 202,
                "customer_id": 10,
                "inventory_id": 2,
                "staff_id": 1,
                "rental_date": "2026-10-18 01:00:00.000",
                "return_date": None,
                "last_update": "2026-10-18 01:00:00.000",
                "cdc_capture_at": "2026-10-18 01:00:00.000",
                "__op": "u",
                "__deleted": "false",
            },
            {
                "rental_id": 202,
                "customer_id": None,
                "inventory_id": None,
                "staff_id": None,
                "rental_date": None,
                "return_date": None,
                "last_update": None,
                "cdc_capture_at": "2026-10-18 01:00:00.000",
                "__op": "d",
                "__deleted": "true",
            },
        ]
        self._execute_merge(same_ts_events)

        # Since it was never inserted, unmatched delete is dropped
        self.cur.execute("SELECT * FROM target_stg_rental WHERE rental_id = 202")
        self.assertIsNone(self.cur.fetchone())

    def test_t04_legitimate_update_setting_column_to_null(self) -> None:
        # Initial INSERT with return_date set
        self._execute_merge(
            [
                {
                    "rental_id": 303,
                    "customer_id": 7,
                    "inventory_id": 3,
                    "staff_id": 1,
                    "rental_date": "2026-10-18 01:00:00.000",
                    "return_date": "2026-10-18 02:00:00.000",
                    "last_update": "2026-10-18 01:00:00.000",
                    "cdc_capture_at": "2026-10-18 01:00:00.000",
                    "__op": "c",
                    "__deleted": "false",
                }
            ]
        )

        # Legitimate UPDATE setting return_date to NULL
        self._execute_merge(
            [
                {
                    "rental_id": 303,
                    "customer_id": 7,
                    "inventory_id": 3,
                    "staff_id": 1,
                    "rental_date": "2026-10-18 01:00:00.000",
                    "return_date": None,
                    "last_update": "2026-10-18 02:30:00.000",
                    "cdc_capture_at": "2026-10-18 02:30:00.000",
                    "__op": "u",
                    "__deleted": "false",
                }
            ]
        )

        self.cur.execute("SELECT * FROM target_stg_rental WHERE rental_id = 303")
        row = self.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row["return_date"], "Legitimate NULL update must be applied")
        self.assertEqual(row["is_deleted"], 0)

    def test_t05_delete_for_nonexistent_key_not_inserted(self) -> None:
        self._execute_merge(
            [
                {
                    "rental_id": 999,
                    "customer_id": None,
                    "inventory_id": None,
                    "staff_id": None,
                    "rental_date": None,
                    "return_date": None,
                    "last_update": None,
                    "cdc_capture_at": "2026-10-18 03:00:00.000",
                    "__op": "d",
                    "__deleted": "true",
                }
            ]
        )
        self.cur.execute("SELECT * FROM target_stg_rental WHERE rental_id = 999")
        self.assertIsNone(self.cur.fetchone())

    def test_t06_delete_then_recreate_reactivates_record(self) -> None:
        self.test_t01_insert_then_delete_preserves_historical_payload()

        # Re-create rental 101 with strictly newer timestamp (04:00 > 02:30)
        self._execute_merge(
            [
                {
                    "rental_id": 101,
                    "customer_id": 50,
                    "inventory_id": 9,
                    "staff_id": 1,
                    "rental_date": "2026-10-18 04:00:00.000",
                    "return_date": None,
                    "last_update": "2026-10-18 04:00:00.000",
                    "cdc_capture_at": "2026-10-18 04:00:00.000",
                    "__op": "c",
                    "__deleted": "false",
                }
            ]
        )

        self.cur.execute("SELECT * FROM target_stg_rental WHERE rental_id = 101")
        row = self.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["is_deleted"], 0, "Record must be reactivated")
        self.assertEqual(row["customer_id"], 50)
        self.assertEqual(row["cdc_capture_at"], "2026-10-18 04:00:00.000")

    def test_t07_t08_t09_fact_retention_active_views_and_revenue_isolation(
        self,
    ) -> None:
        # Seed Fact table with active payment and soft-deleted payment
        self.cur.execute("""
            INSERT INTO target_fct_payment VALUES
            (1, 10, 1, 101, 10.00, '2026-10-18 01:00:00.000', '2026-10-18 01:00:00.000', 0),
            (2, 20, 1, 102, 15.00, '2026-10-18 01:00:00.000', '2026-10-18 02:00:00.000', 1)
        """)
        self.conn.commit()

        # T-07: Historical facts retain both records
        self.cur.execute("SELECT COUNT(*) as cnt FROM target_fct_payment")
        self.assertEqual(self.cur.fetchone()["cnt"], 2)

        # T-08: Active view filters soft-deleted records
        self.cur.execute("SELECT * FROM target_fct_payment WHERE is_deleted = 0")
        active_rows = self.cur.fetchall()
        self.assertEqual(len(active_rows), 1)
        self.assertEqual(active_rows[0]["payment_id"], 1)

        # T-09: Revenue calculation includes only active payment ($10.00)
        self.cur.execute(
            "SELECT COALESCE(SUM(amount), 0.0) as revenue FROM target_fct_payment WHERE is_deleted = 0"
        )
        self.assertEqual(self.cur.fetchone()["revenue"], 10.00)


if __name__ == "__main__":
    unittest.main()
