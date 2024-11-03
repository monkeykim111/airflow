from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import regist
import pendulum

with DAG(
    dag_id='dag_python_with_op_args',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    task_regist = PythonOperator(
        task_id='task_regist',
        python_callable=regist,
        op_args=['kim',' man', 'kr', 'seoul']
    )

    task_regist

