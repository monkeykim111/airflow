from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from common.climaml_data_utils import fetch_weather_data
import pendulum
import pandas as pd
import numpy as np


class ClimamlFetchHistoricalWeatherDataOperator(BaseOperator):
    template_fields = ('start_date', 'end_date')  # 템플릿 필드 등록

    def __init__(self, conn_id, start_date, end_date, station_ids, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.start_date = start_date
        self.end_date = end_date
        self.station_ids = station_ids

    def execute(self, context):
        # 템플릿 문자열을 pendulum datetime으로 변환
        try:
            start_date = pendulum.parse(self.start_date)
            end_date = pendulum.parse(self.end_date)
        except Exception as e:
            self.log.error(f"날짜 변환 중 오류 발생: {e}")
            raise

        postgres_hook = PostgresHook(postgres_conn_id=self.conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()

        current_start = start_date
        request_count = 0
        max_requests_per_day = 10000

        while current_start <= end_date:
            if request_count >= max_requests_per_day:
                self.log.info("일일 최대 API 요청 수에 도달했습니다. 동작을 중단합니다.")
                break

            end_of_period = min(current_start.add(days=90), end_date)
            params_base = {
                'serviceKey': Variable.get("data_go_kr"),
                'pageNo': '1',
                'numOfRows': '999',
                'dataType': 'JSON',
                'dataCd': 'ASOS',
                'dateCd': 'DAY',
                'startDt': current_start.format('YYYYMMDD'),
                'endDt': end_of_period.format('YYYYMMDD'),
            }

            try:
                df = fetch_weather_data(params_base=params_base, station_ids=self.station_ids, url='http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList')
                request_count += len(self.station_ids)
                df = df.drop_duplicates()
                df.replace("", np.nan, inplace=True)
                df.to_sql('clima_ml_weather_data', engine, if_exists='append', index=False)

                self.log.info(f"Inserted data for range {current_start.format('YYYY-MM-DD')} to {end_of_period.format('YYYY-MM-DD')}.")

            except Exception as e:
                self.log.error(f"데이터 처리 중 오류 발생: {e}")
                break

            current_start = end_of_period.add(days=1)

        self.log.info(f"Completed processing data from {start_date.format('YYYY-MM-DD')} to {end_date.format('YYYY-MM-DD')}.")
