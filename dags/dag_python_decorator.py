from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="dag_python_decorator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id="python_task_1")
    def print_context(arg):
        print(arg)

    python_task_1 = print_context("############ task decorator 실행 ############")
