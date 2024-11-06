from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dag_bash_with_variable",
    schedule="10 9 * * *", 
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_var = BashOperator(
        task_id='bash_var',
        bash_command="echo variable:{{var.value.sample_key}}"  # var.value로 variable의 value로 접근 가능
    )