from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.decorators import task


with DAG(
    dag_id="dag_python_with_trigger_rule_eg1",
    schedule="10 9 * * *", 
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_upstream_1 = BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo bash_upstream_1'
    )

    @task(task_id='python_upstream_1')
    def python_upstream_1():
        raise AirflowException('Downstream_1_Exception!')
    
    @task(task_id='python_upstream_2')
    def python_upstream_2():
        print('python_upstream_2 정상 처리')

    @task(task_id='python_downstream_1', trigger_rule='all_done')
    def python_downstream_1():
        print('python_downstream_1 정상 처리')

    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()