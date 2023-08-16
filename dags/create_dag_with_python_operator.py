import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "kauas",
    "retries": 5,
    "retries_delay": timedelta(minutes=5)
}


def get_name(ti):
    ti.xcom_push(key="first_name", value="John")
    ti.xcom_push(key="last_name", value="Doe")


def get_age(ti):
    ti.xcom_push(key="age", value=17)


def greet(random_n: int, ti):  # ti -> task instance
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")
    # xcom maximum size: 48Kb
    print(f"Hello! My name is {first_name} {last_name}, I am {age} years old...")
    print(f"Random number: {random_n}")


with DAG(
    dag_id="our_dag_with_python_operator_v3",
    default_args=default_args,
    start_date=datetime(2023, 8, 15, 0),
    schedule_interval="@daily"
) as dag:
    
    task1 = PythonOperator(
        task_id="first_task",
        python_callable=greet,
        op_kwargs={
            "random_n": random.randint(0, 100)
        }  # Pass args to python callable function
    )

    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id="get_age",
        python_callable=get_age
    )
    
    [task2, task3] >> task1
