from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import regist2
import pendulum

with DAG(
    dag_id='dag_python_with_op_kwargs',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    task_regist2 = PythonOperator(
        task_id='task_regist2',
        python_callable=regist2,
        op_args=['kim',' man', 'kr', 'seoul'],
        op_kwargs={'email': 'kwj102501@namver.com', 'phone': '010'}
    )

    task_regist2

