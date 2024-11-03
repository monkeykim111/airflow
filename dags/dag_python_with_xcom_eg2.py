from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dag_python_with_xcom_eg2',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'
    
    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_id='python_xcom_push_by_return')
        print(f'xcom_pull 메서드로 직접 찾은 리턴 값: {value1}')


    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print(f'함수 입력값으로 받은 값: {status}')


        
