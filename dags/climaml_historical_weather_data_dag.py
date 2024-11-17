from airflow import DAG
from datetime import datetime, timedelta
from operators.climaml_fetch_historical_weather_data_operator import ClimamlFetchHistoricalWeatherDataOperator
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='climaml_historical_weather_data_dag',
    default_args= default_args,
    description='10년치 여러 관측소에서의 기상 데이터를 fetch하여 Postgresq DB에 적재합니다.',
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 11, 18, tz="Asia/Seoul"),
    catchup=False) as dag:


    climaml_fetch_historical_data = ClimamlFetchHistoricalWeatherDataOperator(
        task_id='climaml_fetch_historical_data',
        conn_id='conn-db-postgres-custom',
        end_date='2024-11-17',
        station_ids=['108', '105', '114', '112', '119', '127', '131', '137', '283', '146', '247', '184', '189']
    )

    climaml_fetch_historical_data




