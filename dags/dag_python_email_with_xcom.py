from airflow import DAG
import pendulum
from airflow.operators.email import EmailOperator
from airflow.decorators import task


with DAG(
    dag_id="dag_python_email_operator",
    schedule="0 8 1 * *", 
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task(task_id='someting_task')
    def soem_logic():
        from random import choice
        return choice(['Success', 'Fail'])  # return 한 값이 xcom으로 전달됨
        

    send_email = EmailOperator(
        task_id='send_email',
        to='kwj102501@naver.com',
        # 메일 제목
        subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some logic 처리결과',  # to', 'subject', 'html_content', 'files'인자는 template 문법 사용 가능
        # 메일 내용
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과는 <br> \
                    {{ ti.xcom_pull(task_ids="someting_task")}} 했습니다 <br>'
    )

    soem_logic() >> send_email