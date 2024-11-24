from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

def check_context(context):
    # Context 내 logical_date 확인
    if 'logical_date' in context:
        print(f"[INFO] logical_date: {context['logical_date']}")
    else:
        print("[ERROR] logical_date not found in context!")

with DAG(
    dag_id='check_logical_date',
    schedule="0 13 * * *",  # 매일 13시에 실행
    start_date=pendulum.datetime(2024, 11, 25, tz='Asia/Seoul'),
    catchup=False,
    default_args={'owner': 'airflow'}
) as dag:
    
    check_task = PythonOperator(
        task_id='check_logical_date_in_context',
        python_callable=check_context,
        provide_context=True  # Context 전달 활성화
    )

    check_task
