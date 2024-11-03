from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dag_python_show_template',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2024, 10, 20, tz="Asia/Seoul"),
    catchup=True
) as dag:
    @task(task_id='python_task')
    def show_template(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_template