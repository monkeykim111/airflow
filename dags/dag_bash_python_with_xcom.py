import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dag_bash_python_with_xcom',
    schedule='10 0 * * *',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status': 'Good', 'data': [1, 2, 3], 'options_cnt': 100}
        return result_dict  # xcom으로 result_dict의 값이 push가 됨
    
    bash_pull = BashOperator(
        task_id='bash_pull',
        env={
            # BashOperator에서 env와 bash_command는 template 문법이 가능하므로 직접적으로 ti 인스턴스에 접근이 가능하다.
            'STATUS': '{{ ti.xcom_pull(task_ids="python_push")["status"] }}',
            'DATA': '{{ ti.xcom_pull(task_ids="python_push")["data"] }}',
            'OPTIONS_CNT': '{{ ti.xcom_pull(task_ids="python_push")["options_cnt"]}}'
        },
        bash_command='echo $STATUS && echo $DATA && echo $OPTIONS_CNT'
    )

    python_push_xcom() >> bash_pull
    

    bash_push = BashOperator(
        task_id='bash_push',
        bash_command='echo PUSH START '
                     '{{ti.xcom_push(key="bash_pushed", value=200)}} && '
                     'echo PUSH COMPLETE'
    )

    @task(task_id='python_pull')
    def python_pull_xcom(**kwargs):
        ti = kwargs['ti']  # kwargs에 있는 ti 인스턴스 가져오기
        status_value = ti.xcom_pull(key='bash_pushed')
        return_value = ti.xcom_pull(task_ids='bash_push')  # task_ids로 xcom pull하므로 return 값으로 들어온 bash operator의 마지막 출력문이 value로 들어온다.
        print('status_value:' + str(status_value))
        print('return_value:' + return_value)

    bash_push >> python_pull_xcom()
