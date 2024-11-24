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
    start_date=pendulum.datetime(2024, 11, 15, tz="Asia/Seoul"),
    catchup=False) as dag:

    # 관측소 번호와 이름
    station_ids = [
        '108',  # 서울
        '105',  # 강릉
        '114',  # 원주
        '112',  # 인천
        '119',  # 수원
        '127',  # 충주
        '131',  # 청주
        '137',  # 상주
        '283',  # 경주
        '146',  # 전주
        '247',  # 남원
        '184',  # 제주
        '189',  # 서귀포
    ]

    climaml_fetch_historical_data = ClimamlFetchHistoricalWeatherDataOperator(
        task_id='climaml_fetch_historical_data',
        conn_id='conn-db-postgres-custom',
        station_ids=station_ids,
    )