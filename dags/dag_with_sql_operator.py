from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime, timedelta


default_args = {
    "onwer": "kauas",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="sql_operator_dag",
    start_date=datetime(2023, 8, 15),
    schedule_interval="*0 0 * * *"
) as dag:
    
    task1 = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
            """
    )

    task2 = SQLExecuteQueryOperator(
        task_id="task2",
        conn_id="postgres_localhost",
        sql="""
            INSERT INTO dags_run (dt, dag_id) VALUES ('{{ ds }}', '{{ dag.dag_id }}')
            """
    )

    task3 = SQLExecuteQueryOperator(
        task_id="task3",
        conn_id="postgres_localhost",
        sql="""
            DELETE FROM dags_run WHERE dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}'
            """
    )

    task1 >> task2 >> task3
