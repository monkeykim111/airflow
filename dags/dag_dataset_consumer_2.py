from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

dataset_dag_dataset_producer_1 = Dataset("dag_dataset_producer_1")
dataset_dag_dataset_producer_2 = Dataset("dag_dataset_producer_2")

with DAG(
    dag_id='dag_dataset_consumer_2',
    schedule=[dataset_dag_dataset_producer_1, dataset_dag_dataset_producer_2],
    start_date=pendulum.datetime(2024, 11, 25, tz='Asia/Seoul'),
    catchup=False
) as dag:
    bask_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ ti.run_id }} && echo "producer_1과 producer_2가 완료되면 수행됨"'
    )