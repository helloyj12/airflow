from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import regist

import pendulum

with DAG(
    dag_id="dags_python_with_op_args",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 6, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    regist_t1 = PythonOperator(
        task_id='regist_t1',
        python_callable=regist,
        op_args=['yeonjaekim', 'female', 'korea', 'jeju']

    )