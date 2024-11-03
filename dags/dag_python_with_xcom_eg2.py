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
    def xcom_push_result():
        return 'Success'
    
    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1():
        from airflow.decorators import get_current_context
        context = get_current_context()
        ti = context['ti']
        value1 = ti.xcom_pull(task_id='python_xcom_push_by_return')
        print(f'xcom_pull 메서드로 직접 찾은 리턴 값: {value1}')

    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status):
        print(f'함수 입력값으로 받은 값: {status}')

    # 태스크 호출 및 의존성 설정
    python_xcom_push_by_return = xcom_push_result()
    python_xcom_pull_2 = xcom_pull_2(python_xcom_push_by_return)
    
    # python_xcom_push_by_return이 xcom_pull_1과 xcom_pull_2의 의존성이 되도록 설정
    python_xcom_push_by_return >> [xcom_pull_1(), python_xcom_pull_2]
