import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 6, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    def select_fruit():
        fruits = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0, 3)
        print(fruits[rand_int])

    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_fruit

    )

    py_t1