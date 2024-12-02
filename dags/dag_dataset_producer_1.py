from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

dataset_dag_dataset_producer_1 = Dataset("dag_dataset_producer_1") # dag_dataset_producer_1은 publish할 key값임

with DAG(
    dag_id='dag_dataset_producer_1',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 4, 1, tz='Asia/seoul'),
    catchup=False
) as dag:
    bask_task = BashOperator(
        task_id='bash_task',
        outlets=[dataset_dag_dataset_producer_1],  # baseOperator 에 정의돼있음 / 
        bash_command='echo "producer_1 job done"'
    )