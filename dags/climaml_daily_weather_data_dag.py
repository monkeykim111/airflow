from airflow import DAG
from datetime import datetime, timedelta
from operators.climaml_fetch_historical_weather_data_operator import ClimamlFetchHistoricalWeatherDataOperator
from sensors.climaml_data_sensor import ClimamlDataSensor
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='climaml_daily_weather_data_dag',
    default_args=default_args,
    description='매일 전날 공공데이터의 기상 데이터를 요청한 후 조회가 가능하면 데이터를 가지고와 postgreSQL에 적재합니다.',
    schedule="0 13 * * *",
    start_date=pendulum.datetime(2024, 11, 25, tz='Asia/Seoul'),
    catchup=False
) as dag:
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

    # 전일 데이터 요청이 가능한지 여부 확인
    check_date = "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y%m%d') }}"
    climaml_check_api_avaliable = ClimamlDataSensor(
        task_id='climaml_check_api_avaliable',
        check_date=check_date,
        url='http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList',
        api_key="{{ var.value.data_go_kr }}",
        station_ids=station_ids,
        poke_interval=300,  # 5분마다 확인
        timeout=3600,  # 최대 1시간 대기
    )

    climaml_fetch_daily_data = ClimamlFetchHistoricalWeatherDataOperator(
        task_id='climaml_fetch_daily_data',
        conn_id='conn-db-postgres-custom',
        start_date="{{ (execution_date - macros.timedelta(days=1)) }}",
        end_date="{{ (execution_date - macros.timedelta(days=1)) }}",
        station_ids=station_ids,
    )

    climaml_check_api_avaliable >> climaml_fetch_daily_data