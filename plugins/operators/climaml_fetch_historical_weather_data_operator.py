from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from common.climaml_data_utils import fetch_weather_data
import pendulum
import pandas as pd
import numpy as np


class ClimamlFetchHistoricalWeatherDataOperator(BaseOperator):
    def __init__(self, conn_id, station_ids, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.station_ids = station_ids

    def execute(self, context):
        # Airflow context에서 logical_date 가져오기
        logical_date = context['logical_date']
        data_interval_start = context['data_interval_start']
        data_interval_end = context['data_interval_end']
        print(f"Logical Date: {logical_date}")
        print(f"Data Interval Start: {data_interval_start}")
        print(f"Data Interval End: {data_interval_end}")
        start_date = logical_date.subtract(days=1).strftime('%Y-%m-%d')
        end_date = start_date  # 동일한 날짜 사용

        # PostgreSQL 연결 설정
        postgres_hook = PostgresHook(postgres_conn_id=self.conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()

        # API 기본 설정
        api_key = Variable.get("data_go_kr")
        url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'

        current_start = pendulum.parse(start_date)
        current_end = pendulum.parse(end_date)

        request_count = 0
        max_requests_per_day = 10000

        while current_start <= current_end:
            if request_count >= max_requests_per_day:
                self.log.info("일일 최대 API 요청 수에 도달했습니다. 동작을 중단합니다.")
                break

            end_of_period = min(current_start.add(days=90), current_end)

            params_base = {
                'serviceKey': api_key,
                'pageNo': '1',
                'numOfRows': '999',
                'dataType': 'JSON',
                'dataCd': 'ASOS',
                'dateCd': 'DAY',
                'startDt': current_start.format('YYYYMMDD'),
                'endDt': end_of_period.format('YYYYMMDD'),
            }

            try:
                df = fetch_weather_data(params_base=params_base, station_ids=self.station_ids, url=url)
                request_count += len(self.station_ids)
                df = df.drop_duplicates()
                df.replace("", np.nan, inplace=True)
                df.to_sql('clima_ml_weather_data', engine, if_exists='append', index=False)

                self.log.info(f"Inserted data for range {current_start.format('YYYY-MM-DD')} to {end_of_period.format('YYYY-MM-DD')}.")
            except Exception as e:
                self.log.error(f"데이터 처리 중 오류 발생: {e}")
                break

            current_start = end_of_period.add(days=1)

        self.log.info(f"Completed processing data from {start_date} to {end_date}.")
