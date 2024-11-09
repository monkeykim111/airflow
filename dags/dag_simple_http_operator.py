import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task



with DAG(
    dag_id="dag_simple_http_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    ''' 서울시 강우량 정보 '''
    rain_fall_info = SimpleHttpOperator(
        task_id='rain_fall_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/ListRainfallService/1/5/',
        method='GET',
        headers={'Content-Type': 'application/json',
                 'charset': 'utf-8',
                 'Accept': '*/*'
                 }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='rain_fall_info')  # xcom 값에서 빼옴
        import json
        from pprint import pprint

        pprint(json.loads(result))

    rain_fall_info >> python_2()