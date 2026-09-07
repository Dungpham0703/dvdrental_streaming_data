# pyrefly: ignore [missing-import]
from datetime import datetime, timedelta

from airflow import DAG

# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'tino_dataviet',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_transform_dvdrental_dag',
    default_args=default_args,
    description='A simple DAG to run dbt project every 15 minutes',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dbt', 'transform', 'dvdrental'],
) as dag:

    DBT_PROJECT_DIR = '/opt/airflow/dbt_transform'
    DBT_PROFILES_DIR = '/opt/airflow/dbt_transform'

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
    )
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
    )
    
    dbt_deps >> dbt_run >> dbt_test
