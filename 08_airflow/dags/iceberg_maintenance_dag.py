import os
import time
import json
import logging
from datetime import datetime, timedelta
# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator
from botocore.config import Config
from botocore.exceptions import ClientError

class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("event", "database", "table", "query_id", "state", "duration_sec", "error_code"):
            if hasattr(record, key):
                log_payload[key] = getattr(record, key)

        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_payload, ensure_ascii=False)

logger = logging.getLogger("iceberg_maintenance")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    logger.addHandler(handler)

BOTO_RESILIENT_CONFIG = Config(
    retries={
        'max_attempts': 5,
        'mode': 'adaptive'
    },
    connect_timeout=5,
    read_timeout=15
)

default_args = {
    'owner': 'tino_data',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

DATABASES_TO_MAINTAIN = ['dvdrental_staging', 'dvdrental_mart']

def run_athena_query(client, query: str, output_location: str, database: str, table: str, max_timeout_seconds: int = 900):
    start_time = time.time()
    try:
        response = client.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': output_location}
        )
    
        query_execution_id = response['QueryExecutionId']
        logger.info(
            f"Query Athena has been submited: {query[:80]}...",
            extra={"event": "query_submitted", "database": database, "table": table, "query_id": query_execution_id}
        )
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(
            f"Cannot submit Athena query: {str(e)}",
            extra={"event": "query_submit_failed", "database": database, "table": table, "error_code": error_code},
            exc_info=True
        )
        raise

    sleep_interval = 2.0
    max_interval = 20.0

    while True:
        elapsed = time.time() - start_time
        if elapsed > max_timeout_seconds:
            try:
                client.stop_query_execution(QueryExecutionId=query_execution_id)
                logger.warning(
                    f"Query cancelled due to max timeout {max_timeout_seconds}s",
                    extra={"event": "query_cancelled_timeout", "database": database, "table": table, "query_id": query_execution_id}
                )
            except Exception:
                pass
            raise TimeoutError(f"Athena query {query_execution_id} timeout after {elapsed:.1f}s (limit: {max_timeout_seconds}s)")

        status_response = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status_response['QueryExecution']['Status']['State']

        if state == 'SUCCEEDED':
            logger.info(
                f"Query completed in {elapsed:.2f}s",
                extra={
                    "event": "query_succeeded",
                    "database": database,
                    "table": table,
                    "query_id": query_execution_id,
                    "duration_sec": round(elapsed, 2)
                }
            )
            return True
        elif state in ('FAILED', 'CANCELLED'):
            reason = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown reason')
            logger.error(
                f"Query {state}: {reason}",
                extra={
                    "event": "query_failed",
                    "database": database,
                    "table": table,
                    "query_id": query_execution_id,
                    "state": state
                }
            )
            raise RuntimeError(f"Athena query {query_execution_id} {state}: {reason}")

        time.sleep(sleep_interval)
        sleep_interval = min(sleep_interval * 1.5, max_interval)

def perform_iceberg_maintenance(**kwargs):
    import boto3
    s3_output = os.getenv('AIRFLOW__AWS__S3_BUCKET_NAME')
    aws_region = os.getenv('AWS_REGION')

    glue_client = boto3.client('glue', region_name=aws_region, config=BOTO_RESILIENT_CONFIG)
    athena_client = boto3.client('athena', region_name=aws_region, config=BOTO_RESILIENT_CONFIG)

    failed_tables = []

    for database in DATABASES_TO_MAINTAIN:
        logger.info(f"Starting maintenance for database: {database}", extra={"event": "db_maintenance_start", "database": database})
        try:
            response = glue_client.get_tables(DatabaseName=database)
            tables = response.get('TableList', [])

            if not tables:
                logger.info(f"No tables found in database {database}", extra={"event": "no_tables_found", "database": database})
                continue

            for table in tables:
                table_name = table['Name']
                parameters = table.get('Parameters', {})

                if parameters.get('table_type', '').upper() == 'ICEBERG':
                    logger.info(
                        f"Optimizing table: {database}.{table_name}",
                        extra={"event": "table_maintenance_start", "database": database, "table": table_name}
                    )
                    try:
                        optimize_query = f"OPTIMIZE {database}.{table_name} REWRITE DATA USING BIN_PACK;"
                        max_optimize_runs = 5
                        for run in range(max_optimize_runs):
                            try:
                                run_athena_query(athena_client, optimize_query, s3_output, database, table_name)
                                break
                            except Exception as e:
                                if "ICEBERG_OPTIMIZE_MORE_RUNS_NEEDED" in str(e):
                                    logger.warning(
                                        f"Table {table_name} has too many small files, running another optimization pass...",
                                        extra={"event": "optimize_need_more_runs", "database": database, "table": table_name}
                                    )
                                    continue
                                raise

                        vacuum_query = f"VACUUM {database}.{table_name};"
                        run_athena_query(athena_client, vacuum_query, s3_output, database, table_name)

                        logger.info(
                            f"Completed maintenance for table: {database}.{table_name}",
                            extra={"event": "table_maintenance_completed", "database": database, "table": table_name}
                        )
                    except Exception as tbl_err:
                        logger.error(
                            f"Error when maintaining table {database}.{table_name}: {str(tbl_err)}",
                            extra={"event": "table_maintenance_error", "database": database, "table": table_name},
                            exc_info=True
                        )
                        failed_tables.append(f"{database}.{table_name}")
                else:
                    logger.info(
                        f"Skip table {database}.{table_name} cuz it's not an Iceberg table",
                        extra={"event": "table_skipped", "database": database, "table": table_name}
                    )
        except Exception as db_err:
            logger.error(
                f"Error when accessing database {database}: {str(db_err)}",
                extra={"event": "database_error", "database": database},
                exc_info=True
            )
            failed_tables.append(f"DATABASE:{database}")

    if failed_tables:
        raise RuntimeError(f"Iceberg maintenance completed with {len(failed_tables)} failed tables: {', '.join(failed_tables)}")

with DAG(
    'iceberg_table_maintenance_dag',
    default_args=default_args,
    description='Automated Maintenance Compaction and Vacuum for Apache Iceberg tables',
    schedule_interval='0 2 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['iceberg', 'maintenance', 'athena'],
) as dag:

    maintenance_task = PythonOperator(
        task_id='run_optimize_and_vacuum',
        python_callable=perform_iceberg_maintenance,
        provide_context=True
    )