from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

dataset_dag_dataset_producer_1 = Dataset("dag_dataset_producer_1")

with DAG(
    dag_id='dag_dataset_consumer_2',
    schedule=[dataset_dag_dataset_producer_1], # Dataset 인스턴스를 구독하겠다는 뜻임
    start_date=pendulum.datetime(2024, 4, 1, tz='Asia/seoul'),
    catchup=False
) as dag:
    bask_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ ti.run_id }} && echo "producer_1이 완료되면 수행됨"'
    )