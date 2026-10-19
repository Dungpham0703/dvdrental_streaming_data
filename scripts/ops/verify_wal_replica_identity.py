#!/usr/bin/env python3
"""
verify_wal_replica_identity.py — PostgreSQL CDC Replica Identity Enforcer & Diagnostics
Ensures PostgreSQL WAL includes complete BEFORE images during DELETE operations.
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

TARGET_TABLES = [
    "actor",
    "address",
    "category",
    "city",
    "country",
    "customer",
    "film",
    "film_actor",
    "film_category",
    "inventory",
    "language",
    "payment",
    "rental",
    "staff",
    "store",
]


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "dvdrental"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASS", "postgres"),
    )


def inspect_and_fix_replica_identity():
    try:
        conn = get_connection()
    except Exception as e:
        print(f"[ERROR] Failed to connect to PostgreSQL: {e}")
        print("Please check your DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS in .env")
        sys.exit(1)

    conn.autocommit = True
    cur = conn.cursor(cursor_factory=RealDictCursor)

    print("=" * 65)
    print(" 1. INSPECTING POSTGRESQL REPLICA IDENTITY (relreplident)")
    print("=" * 65)

    query = """
    SELECT
        c.relname AS table_name,
        c.relreplident AS replica_identity_code,
        CASE c.relreplident
            WHEN 'd' THEN 'default (PK only)'
            WHEN 'n' THEN 'nothing'
            WHEN 'f' THEN 'FULL (all columns before-image)'
            WHEN 'i' THEN 'custom index'
        END AS replica_identity_desc
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind = 'r'
      AND c.relname = ANY(%s)
    ORDER BY c.relname;
    """
    cur.execute(query, (TARGET_TABLES,))
    rows = cur.fetchall()

    if not rows:
        print("  [WARN] No matching tables found in schema 'public'.")

    needs_alter = []
    for r in rows:
        status = "✅ OK" if r["replica_identity_code"] == "f" else "⚠️  INCOMPLETE"
        print(f"  - {r['table_name']:<18}: {r['replica_identity_desc']:<32} [{status}]")
        if r["replica_identity_code"] != "f":
            needs_alter.append(r["table_name"])

    if needs_alter:
        print("\n" + "=" * 65)
        print(f" 2. ALTERING {len(needs_alter)} TABLES TO REPLICA IDENTITY FULL")
        print("=" * 65)
        for tbl in needs_alter:
            alter_sql = f"ALTER TABLE public.{tbl} REPLICA IDENTITY FULL;"
            print(f"  Executing: {alter_sql}")
            cur.execute(alter_sql)
        print("  All target tables are now configured with REPLICA IDENTITY FULL.")
    else:
        print("\n  All tables already have REPLICA IDENTITY FULL configured!")

    print("\n" + "=" * 65)
    print(" 3. CDC DELETE INTEGRITY VERIFICATION TEST INSTRUCTIONS")
    print("=" * 65)
    print("""
  To verify full payload preservation in Debezium SMT:
  1. Insert a test record in PostgreSQL:
     INSERT INTO film (title, description, release_year, language_id, rental_duration, rental_rate)
     VALUES ('TEST_DELETE_TITANIC', 'Audit test', 2026, 1, 3, 2.99) RETURNING film_id;

  2. Delete the record immediately:
     DELETE FROM film WHERE title = 'TEST_DELETE_TITANIC';

  3. Inspect Kafka topic 'cdc.public.film' via console consumer:
     docker compose -f infra/docker/kafka/docker-compose.yml exec kafka \\
       kafka-console-consumer --bootstrap-server localhost:9092 \\
       --topic cdc.public.film --from-beginning | grep "TEST_DELETE_TITANIC"

  4. Expected SMT Output Verification:
     - Record MUST have: "__op": "d", "__deleted": "true"
     - Record MUST preserve: "title": "TEST_DELETE_TITANIC", "rental_rate": 2.99
     - Record MUST NOT have nullified business attributes!
    """)
    cur.close()
    conn.close()


if __name__ == "__main__":
    inspect_and_fix_replica_identity()
