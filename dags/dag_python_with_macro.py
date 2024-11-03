from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dag_python_with_macro',
    schedule='10 0 * * *',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    @task(task_id='task_using_macro',
    templates_dict={'start_date': '{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
                    'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds}}'
                    })
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get(templates_dict) or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'start_date 없음'
            end_date = templates_dict.get('end_date') or 'end_date 없음'
            print(start_date)
            print(end_date)

    get_datetime_macro()