from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id='dag_bash_with_macro_eg1',
    schedule='10 0 L * *',  # 마지막 날
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    # start date: 전월 말일, end date: 1일 전
    bash_t1 = BashOperator(
        task_id='bash_t1',
        env={
            'START_DATE':'{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}', # 한국시간대
            'END_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds }}'
        },
        bash_command='echo "START_DATE: $START_DATE" && "END_DATE: $END_DATE"'
    )