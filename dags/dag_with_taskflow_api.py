from airflow.decorators import dag, task
from datetime import datetime, timedelta


default_args = {
    "owner": "kauas",
    "retries": 5,
    "retries_delay": timedelta(minutes=2)
}


@dag(
    dag_id="dag_with_taskflow_api_v2",
    default_args=default_args,
    start_date=datetime(2023, 8, 15, 0),
    schedule_interval="@daily"
)
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name": "John",
            "last_name": "Doe"
        }

    @task()
    def get_age():
        return 17

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello, my name is {first_name} {last_name}, i'm {age} years old...")

    name_dict = get_name()
    age = get_age()
    greet(
        first_name=name_dict["first_name"],
        last_name=name_dict["last_name"],
        age=age
    )


greet_dag = hello_world_etl()
