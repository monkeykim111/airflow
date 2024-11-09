from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup


with DAG(
    dag_id="dag_python_with_task_group",
    schedule="10 9 * * *", 
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def inner_func(**kwargs):
          msg = kwargs.get('msg') or ''
          print(msg)

    @task_group(group_id='first_group')
    def group_1():
        """task group 데코레이터를 사용한 첫번째 그룹입니다."""
        # task group 데코레이션에서 입력한 docstring은 task group의 tooltip으로서 설명을 제공한다.

        @task(task_id='inner_func1')  
        def inner_func1(**kwargs):
             print('첫번째 task group의 첫번째 task 입니다.')

        inner_func2 = PythonOperator(
             task_id='inner_func2',
             python_callable=inner_func,
             op_kwargs={'msg': '첫번째 task group의 두번째 task 입니다.'}
        )

        inner_func1() >> inner_func2

    with TaskGroup(group_id='second_group', tooltip='두번째 그룹입니다.') as group_2:
        ''' 이곳에 적은 docstring은 표시되지 않습니다. '''  # task group 데코레이션을 사용하지 않고 적은 docstring은 표시되지 않고 tooltip으로 적은 것만 보이게 된다.
        @task(task_id='inner_func1')  # 서로 다른 task group 내 task id는 같아도 된다.
        def inner_func1(**kwargs):
              print('두번째 task group 내 첫번째 task 입니다.')

        inner_func2 = PythonOperator(
             task_id='inner_func2',
             python_callable=inner_func,
             op_kwargs={'msg': '두번째 task group 내 두번째 task 입니다.'}
        )
        inner_func1() >> inner_func2

    group_1() >> group_2  # task group의 flow도 지정할 수 있다.