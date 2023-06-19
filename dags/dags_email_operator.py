from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *", # 매월 1일 아침 8시
    start_date=pendulum.datetime(2023, 6, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='yeonj_kim@naver.com',
        cc='helloyj12@naver.com', # 참조
        subject='Airflow 성공 알림',
        html_content='Airflow 작업이 완료되었습니다.',
    )