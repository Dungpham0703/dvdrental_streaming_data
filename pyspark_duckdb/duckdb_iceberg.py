from pandas.core.arrays import categorical
import duckdb
# pyrefly: ignore [missing-import]
from pyiceberg.catalog import load_catalog
import os

def query_iceberg_with_duckdb():
    print("Initializing DuckDB...")
    
    con = duckdb.connect(database=':memory:')

    catalog = load_catalog("dzung_duckdb", type="glue")

    namespaces = catalog.list_namespaces()
    for ns in namespaces:
        print(ns) 
    print()

    table = catalog.load_table("dvdrental_staging.stg_actor")
    print(table)

    # 3. Querying Iceberg tables
    print("\nDuckDB is ready to query Iceberg tables via PyArrow!")
    arrow_table = table.scan().to_arrow()
    print(arrow_table)
    df = con.execute("SELECT * FROM arrow_table LIMIT 5").df()
    print(df)
    
    return con

if __name__ == "__main__":
    con = query_iceberg_with_duckdb()
    con.close()
