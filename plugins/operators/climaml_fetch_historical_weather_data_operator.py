from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from common.climaml_data_utils import fetch_weather_data
import pendulum
import pandas as pd
import numpy as np


class ClimamlFetchHistoricalWeatherDataOperator(BaseOperator):
    template_fields = ('start_date', 'end_date')

    def __init__(self, conn_id, start_date, end_date, station_ids, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.start_date = start_date  
        self.end_date = end_date 
        self.station_ids = station_ids

    def execute(self, context):
        start_date = pendulum.parse(self.start_date)
        end_date = pendulum.parse(self.end_date)

        # PostgreSQL 연결 설정
        postgres_hook = PostgresHook(postgres_conn_id=self.conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()

        # API 기본 설정
        api_key = Variable.get("data_go_kr")  # Airflow Variable에서 API 키 가져오기
        url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'

        # 데이터 요청 및 처리
        current_start = start_date

        while current_start <= end_date:
            end_of_period = min(current_start.add(days=90), end_date)

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
                df = df.drop_duplicates()
                df.replace("", np.nan, inplace=True)
                df.to_sql('clima_ml_weather_data', engine, if_exists='append', index=False)

                self.log.info(f"Inserted data for range {current_start.format('YYYY-MM-DD')} to {end_of_period.format('YYYY-MM-DD')}.")
            except Exception as e:
                self.log.error(f"데이터 처리 중 오류 발생: {e}")
                break

            current_start = end_of_period.add(days=1)

        self.log.info(f"Completed processing data from {start_date} to {end_date}.")
