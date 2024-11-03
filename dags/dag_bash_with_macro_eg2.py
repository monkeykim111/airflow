from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id='dag_bash_with_macro_eg2',
    schedule='10 0 * * 6#2',  # 매월 두번째 토요일
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    # start date: 2주전 월요일, end date: 2주전 토요일
    # 4월 8일이 토요일이라면,
    # 3월 20일이 2주전 월요일이 되고 (start date) (배치일(매월 두번째 토요일) 기준 -19일)
    # 3월 25일이 2주전 토요일이 된다. (end date)  ) (배치일(매월 두번째 토요일) 기준 -14일을 하면 2주전 토요일이 나옴)
    bash_t2 = BashOperator(
        task_id='bash_t2',
        env={
            'START_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds }}',
            'END_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14)) | ds }}'
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )