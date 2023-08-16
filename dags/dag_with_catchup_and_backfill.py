from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "kauas",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_catchup_backfill_v1",
    start_date=datetime(2023, 8, 1),
    schedule_interval="0 0 * * *",
    catchup="False" 
    # catchup=True => Airflow starts processing, including past intervals
    # catchup=False => Airflow starts processing from the current interval
) as dag:
    
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo This is a simple bash command!"
    )

# Backfill
# If for some reason we want to re-run DAGs on certain schedules manually we can use the following CLI command to do so.

# airflow dags backfill --start-date START_DATE --end-date END-DATE dag_id

# This will execute all DAG runs that were scheduled between START_DATE & END_DATE irrespective of the value of the catchup parameter in airflow.cfg.