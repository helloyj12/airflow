import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg2",
    schedule="10 0 * * 6#2", # 격주 토요일마다
    start_date=pendulum.datetime(2023, 6, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    bash_task_2 = BashOperator(
        task_id='bash_task_2',
        env={'START_DATE':'{{ (data_interval_end.in_timezone("Asiz/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds }}', # 2주전 월요일
             'END_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(weeks=2)) | ds}}'}, # 2주전 토요일
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )
