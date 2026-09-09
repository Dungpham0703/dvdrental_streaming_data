import os
import time
import logging
from datetime import datetime, timedelta
# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'tino_data',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

DATABASES_TO_MAINTAIN = ['dvdrental_staging', 'dvdrental_mart']

def run_athena_query(client, query, output_location):
    logging.info(f"Executing query: {query}")
    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': output_location}
    )
    
    query_execution_id = response['QueryExecutionId']
    
    while True:
        status_response = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status_response['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED']:
            logging.info(f"Query succeeded: {query}")
            return True
        elif state in ['FAILED', 'CANCELLED']:
            reason = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown reason')
            logging.error(f"Query failed/cancelled: {query}. Reason: {reason}")
            raise Exception(f"Athena query failed: {reason}")
        time.sleep(5)

def perform_iceberg_maintenance(**kwargs):
    import boto3
    s3_output = os.getenv('AIRFLOW__AWS__S3_BUCKET_NAME')
    aws_region = os.getenv('AWS_REGION')
    glue_client = boto3.client('glue', region_name=aws_region)
    athena_client = boto3.client('athena', region_name=aws_region)
    
    for database in DATABASES_TO_MAINTAIN:
        logging.info(f"=== Start maintaining database: {database} ===")
        try:
            response = glue_client.get_tables(DatabaseName=database)
            tables = response.get('TableList', [])
            
            if not tables:
                logging.info(f"No tables found in database {database}")
                continue
                
            for table in tables:
                table_name = table['Name']
                parameters = table.get('Parameters', {})
                
                if parameters.get('table_type', '').upper() == 'ICEBERG':
                    logging.info(f"--- Maintaining table: {database}.{table_name} ---")
                    
                    optimize_query = f"OPTIMIZE {database}.{table_name} REWRITE DATA USING BIN_PACK;"
                    
                    max_optimize_runs = 5
                    for run in range(max_optimize_runs):
                        try:
                            run_athena_query(athena_client, optimize_query, s3_output)
                            break
                        except Exception as e:
                            if "ICEBERG_OPTIMIZE_MORE_RUNS_NEEDED" in str(e):
                                logging.warning("Table has too many files. Retrying...")
                                continue

                    vacuum_query = f"VACUUM {database}.{table_name};"
                    run_athena_query(athena_client, vacuum_query, s3_output)
                    
                    logging.info(f"--- Completed maintaining table: {database}.{table_name} ---")                    
                else:
                    logging.info(f"Skipping table {database}.{table_name} because it is not an Iceberg table.")
        except Exception as e:
            logging.error(f"Error while processing database {database}: {str(e)}")
            raise e

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

