import os
import sys
import time

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "postgres")
TARGET_SIZE_GB = float(os.environ.get("TARGET_SIZE_GB", "10.0"))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)


def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    conn.autocommit = True
    return conn


def get_db_size_gb(cur):
    cur.execute(f"SELECT pg_database_size('{DB_NAME}') / (1024.0 * 1024 * 1024)")
    return cur.fetchone()[0]


def fast_pump():
    print("=" * 65, flush=True)
    print(f"[+] ULTRA FAST RESILIENT DATABASE PUMP TO {TARGET_SIZE_GB} GB", flush=True)
    print(f"[+] Target DB: '{DB_NAME}' on {DB_HOST}", flush=True)
    print("=" * 65, flush=True)

    conn = None
    cur = None

    def reconnect():
        nonlocal conn, cur
        if cur:
            try:
                cur.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SET session_replication_role = 'replica';")
        except Exception:
            pass

    reconnect()

    initial_size = get_db_size_gb(cur)
    print(f"[+] Current DB Size: {initial_size:.2f} GB", flush=True)

    if initial_size >= TARGET_SIZE_GB:
        print(
            f"[+] DB size is already {initial_size:.2f} GB (>= {TARGET_SIZE_GB} GB). Nothing to do!",
            flush=True,
        )
        cur.close()
        conn.close()
        return

    # Fetch max IDs for safe FK assignment
    cur.execute("SELECT COALESCE(MAX(customer_id), 50000) FROM customer")
    max_cust = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(MAX(inventory_id), 10000) FROM inventory")
    max_inv = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(MAX(staff_id), 2) FROM staff")
    max_staff = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(MAX(rental_id), 1) FROM rental")
    max_rental = cur.fetchone()[0]

    print(
        f"FK Bounds -> Customer: 1..{max_cust} | Inventory: 1..{max_inv} | Staff: 1..{max_staff} | Rental: 1..{max_rental}",
        flush=True,
    )

    # 250,000 rows per batch ensures 1-2 second execution per batch with zero RDS strain
    CHUNK_SIZE = 250000
    batch_num = 1
    start_time = time.time()

    while True:
        try:
            current_size = get_db_size_gb(cur)
            elapsed = time.time() - start_time
            print(
                f"\n[Batch {batch_num}] DB Size: {current_size:.2f} GB / {TARGET_SIZE_GB:.2f} GB (Elapsed: {elapsed:.1f}s)",
                flush=True,
            )

            if current_size >= TARGET_SIZE_GB:
                print(
                    f"\n[SUCCESS] Target reached! Final DB Size: {current_size:.2f} GB in {elapsed:.1f} seconds!",
                    flush=True,
                )
                break

            print(
                f"[+] Injecting {CHUNK_SIZE:,} rows into 'rental' and 'payment'...",
                flush=True,
            )
            batch_start = time.time()
            seed_offset = int(time.time() * 1000) % 1000000

            # 1. Bulk insert rental
            sql_rental = f"""
            INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
            SELECT
                NOW() - ((g + {seed_offset})::text || ' seconds')::interval,
                (g % {max_inv}) + 1,
                (g % {max_cust}) + 1,
                (g % {max_staff}) + 1
            FROM generate_series(1, {CHUNK_SIZE}) AS g
            ON CONFLICT DO NOTHING;
            """
            cur.execute(sql_rental)

            # Refresh max_rental
            cur.execute("SELECT COALESCE(MAX(rental_id), 1) FROM rental")
            max_rental = cur.fetchone()[0]

            # 2. Bulk insert payment
            sql_payment = f"""
            INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date)
            SELECT
                (g % {max_cust}) + 1,
                (g % {max_staff}) + 1,
                (g % {max_rental}) + 1,
                ROUND((random() * 50 + 1)::numeric, 2),
                NOW() - ((g + {seed_offset})::text || ' seconds')::interval
            FROM generate_series(1, {CHUNK_SIZE}) AS g;
            """
            cur.execute(sql_payment)

            batch_elapsed = time.time() - batch_start
            new_size = get_db_size_gb(cur)
            added_gb = new_size - current_size
            print(
                f"  └─ Batch {batch_num} finished in {batch_elapsed:.1f}s | Added +{added_gb:.2f} GB | Current Size: {new_size:.2f} GB",
                flush=True,
            )
            batch_num += 1
            time.sleep(0.5)

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as err:
            print(
                f"\n[!] Connection dropped ({err}). Auto-reconnecting in 3 seconds...",
                flush=True,
            )
            time.sleep(3)
            try:
                reconnect()
            except Exception as rec_err:
                print(
                    f"[!] Reconnect attempt failed ({rec_err}), retrying...", flush=True
                )

    if cur:
        cur.close()
    if conn:
        conn.close()


if __name__ == "__main__":
    fast_pump()
