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
    dag_id="dag_with_cron_expression_v1",
    start_date=datetime(2023, 8, 15),
    schedule_interval="0 3 * * Tue-Fri"
) as dag:
    
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo Dag with cron expression"
    )

    task1