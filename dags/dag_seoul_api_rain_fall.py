import pendulum
from airflow import DAG
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow.decorators import task



with DAG(
    dag_id="dag_seoul_api_rain_fall",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    ''' 서울시 강우량 정보 '''
    rain_fall_info = SeoulApiToCsvOperator(
        task_id='rain_fall_info',
        dataset_name='ListRainfallService',
        path='/opt/airflow/files/ListRainfallService/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='ListRainfallService.csv'
    )

    rain_fall_info