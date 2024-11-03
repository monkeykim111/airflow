from airflow import DAG
from airflow.operators.bash import BaseOperator
import pendulum

with DAG(
    dag_id='dag_bash_with_template',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_t1 = BaseOperator(
        task_id='bash_t1',
        bash_command='echo "data_interval_end: {{ data_interval_end }}"'
    )

    bash_t2 = BaseOperator(
        task_id='bash_t2',
        env={
            'START_DATE':'{{data_interval_start | ds}}',
            'END_DATE':'{{data_interval_end | ds}}'
        },
        bash_command='echo $START_DATE && echo $END_DATE'
    )

    bash_t1 >> bash_t2
