# pyrefly: ignore [missing-import]
from datetime import datetime, timedelta
# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'tino_data',
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,           
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=12),
}

with DAG(
    'dbt_transform_dvdrental_dag',
    default_args=default_args,
    description='Production Lakehouse dbt Pipeline: Staging -> Snapshot SCD2 -> Mart -> Test',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'transform', 'dvdrental', 'lakehouse', 'production'],
) as dag:

    dbt_project_dir = '/opt/airflow/dbt_transform'
    dbt_profiles_dir = '/opt/airflow/dbt_transform'
    common_flags = f'--project-dir {dbt_project_dir} --profiles-dir {dbt_profiles_dir} --target-path /tmp/dbt_target'

    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'dbt run {common_flags} --select staging'
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'dbt snapshot {common_flags}'
    )

    dbt_run_mart = BashOperator(
        task_id='dbt_run_mart',
        bash_command=f'dbt run {common_flags} --select mart'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'dbt test {common_flags}'
    )

    dbt_run_staging >> dbt_snapshot >> dbt_run_mart >> dbt_test

