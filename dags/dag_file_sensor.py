from airflow import DAG
from airflow.sensors.filesystem import FileSensor
import pendulum


with DAG(
    dag_id='dag_file_sensor',
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 11, 15, tz="Asia/Seoul"),
    catchup=False
) as dag:
    rainfall_sensor = FileSensor(
        task_id='rainfall_sensor',
        fs_conn_id='conn_file_airflow_files',
        filepath='ListRainfallService/istRainfallService.csv',
        recursive=False,
        poke_interval=60,
        timeout=60*60*24,
        mode='reschedule'
    )

