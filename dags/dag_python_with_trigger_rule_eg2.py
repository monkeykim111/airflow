from airflow import DAG
import pendulum
from airflow.decorators import task


with DAG(
    dag_id="dag_python_with_trigger_rule_eg2",
    schedule="10 9 * * *", 
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task.branch(task_id='python_branch_task')
    def select_random():
        import random

        item_list = ['A', 'B', 'C']
        selected_item = random.choice(item_list)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B', 'C']:
            return ['task_b', 'task_c']

    @task(task_id='task_a')
    def task_a():
        print('task a 정상처리')

    @task(task_id='task_b')
    def task_b():
        print('task b 정상처리')

    @task(task_id='task_c')
    def task_c():
        print('task c 정상처리')

    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print('task d 정상처리')

    select_random() >> [task_a(), task_b(), task_c()] >> task_d()