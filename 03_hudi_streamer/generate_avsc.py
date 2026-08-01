"""
Auto-generate Avro (.avsc) schemas from PostgreSQL information_schema.
Usage:
  python generate_avsc.py \
    --host <DB_HOST> --port 5432 \
    --user <DB_USER> --password <DB_PASS> \
    --dbname <DB_NAME> \
    --output-dir ./schemas
"""

import argparse
import json
import os
import psycopg2

# -------------------------------------------------------
# PostgreSQL type  →  Avro type mapping
# -------------------------------------------------------
PG_TO_AVRO = {
    # integers
    "smallint":           "int",
    "integer":            "int",
    "int":                "int",
    "bigint":             "long",
    "serial":             "int",
    "bigserial":          "long",
    "smallserial":        "int",

    # floats / decimals
    "real":               "float",
    "double precision":   "double",
    "numeric":            "double",
    "decimal":            "double",
    "money":              "double",

    # booleans
    "boolean":            "boolean",

    # strings / text
    "character varying":  "string",
    "character":          "string",
    "varchar":            "string",
    "char":               "string",
    "text":               "string",
    "uuid":               "string",
    "json":               "string",
    "jsonb":              "string",
    "xml":                "string",
    "tsvector":           "string",
    "tsquery":            "string",
    "name":               "string",
    "USER-DEFINED":       "string",   # enums, custom types
    "ARRAY":              "string",   # arrays stored as string

    # date / time — stored as string to avoid Avro logical-type complexity
    "date":               "string",
    "time without time zone":      "string",
    "time with time zone":         "string",
    "timestamp without time zone": "string",
    "timestamp with time zone":    "string",
    "interval":                    "string",

    # binary
    "bytea":              "bytes",
}


def pg_to_avro_type(pg_type: str, nullable: bool, col_name: str, table_name: str, record_keys: set):
    """Return an Avro type (string or union list) for a given Postgres type."""
    avro_base = PG_TO_AVRO.get(pg_type)
    if avro_base is None:
        print(f"  [WARN] Unknown PG type '{pg_type}' for {table_name}.{col_name} → defaulting to 'string'")
        avro_base = "string"

    # Record key fields MUST be non-nullable (Hudi requirement)
    if col_name in record_keys:
        return avro_base  # non-nullable

    if nullable:
        return ["null", avro_base]  # nullable union
    return avro_base


# Record key for each table (matches .properties hoodie.datasource.write.recordkey.field)
TABLE_RECORD_KEYS = {
    "actor":         {"actor_id"},
    "address":       {"address_id"},
    "category":      {"category_id"},
    "city":          {"city_id"},
    "country":       {"country_id"},
    "customer":      {"customer_id"},
    "film":          {"film_id"},
    "film_actor":    {"actor_id", "film_id"},
    "film_category": {"film_id", "category_id"},
    "inventory":     {"inventory_id"},
    "language":      {"language_id"},
    "payment":       {"payment_id"},
    "rental":        {"rental_id"},
    "staff":         {"staff_id"},
    "store":         {"store_id"},
}

TABLES = list(TABLE_RECORD_KEYS.keys())


def generate_schemas(host, port, user, password, dbname, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=dbname
    )
    cur = conn.cursor()

    placeholders = ",".join(["%s"] * len(TABLES))
    cur.execute(f"""
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN ({placeholders})
        ORDER BY table_name, ordinal_position
    """, TABLES)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Group by table
    from collections import defaultdict
    tables = defaultdict(list)
    for table_name, col_name, data_type, is_nullable in rows:
        tables[table_name].append((col_name, data_type, is_nullable == "YES"))

    for table_name, columns in tables.items():
        record_keys = TABLE_RECORD_KEYS.get(table_name, set())

        fields = []
        for col_name, data_type, nullable in columns:
            avro_type = pg_to_avro_type(data_type, nullable, col_name, table_name, record_keys)
            field = {"name": col_name, "type": avro_type}
            if isinstance(avro_type, list) and avro_type[0] == "null":
                field["default"] = None
            fields.append(field)

        # Append ts_ms field (injected by Debezium ExtractNewRecordState add.fields=ts_ms)
        # Named __ts_ms when using default prefix
        fields.append({
            "name": "__ts_ms",
            "type": ["null", "long"],
            "default": None
        })

        schema = {
            "type": "record",
            "name": table_name,
            "namespace": "public",
            "fields": fields
        }

        out_path = os.path.join(output_dir, f"{table_name}.avsc")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2)
        print(f"  ✓ Generated: {out_path}")

    print(f"\nDone! {len(tables)} schema(s) written to '{output_dir}'")


def main():
    parser = argparse.ArgumentParser(description="Generate Avro schemas from PostgreSQL")
    parser.add_argument("--host",     required=True)
    parser.add_argument("--port",     default=5432, type=int)
    parser.add_argument("--user",     required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--dbname",   required=True)
    parser.add_argument("--output-dir", default="./schemas")
    args = parser.parse_args()

    generate_schemas(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
