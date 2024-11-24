from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from common.climaml_data_utils import fetch_weather_data
import pendulum
import pandas as pd
import numpy as np


class ClimamlFetchHistoricalWeatherDataOperator(BaseOperator):
    # 템플릿 필드 등록
    template_fields = ('start_date', 'end_date')

    def __init__(self, conn_id, start_date, end_date, station_ids, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.start_date = start_date  # 템플릿 문자열로 처리
        self.end_date = end_date  # 템플릿 문자열로 처리
        self.station_ids = station_ids

    def execute(self, context):
        # 템플릿에서 변환된 값을 pendulum datetime으로 처리
        start_date = pendulum.parse(self.start_date)  # 템플릿 문자열을 pendulum 객체로 변환
        end_date = pendulum.parse(self.end_date)

        # PostgreSQL 연결 설정
        postgres_hook = PostgresHook(postgres_conn_id=self.conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()

        # API 기본 설정
        api_key = Variable.get("data_go_kr")  # Airflow Variable에서 API 키 가져오기
        url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'

        # 반복문 초기화
        current_start = start_date
        request_count = 0
        max_requests_per_day = 10000

        # 데이터 처리 반복문
        while current_start <= end_date:
            if request_count >= max_requests_per_day:
                self.log.info("일일 최대 API 요청 수에 도달했습니다. 동작을 중단합니다.")
                break

            # 90일 구간으로 나눠 요청
            end_of_period = min(current_start.add(days=90), end_date)

            # API 요청 파라미터 설정
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

            # API 요청 및 데이터 처리
            df = fetch_weather_data(params_base=params_base, station_ids=self.station_ids, url=url)
            request_count += len(self.station_ids)

            self.log.info(f"[INFO] Processing data from {current_start.format('YYYY-MM-DD')} to {end_of_period.format('YYYY-MM-DD')}.")
            df = df.drop_duplicates()
            df.replace("", np.nan, inplace=True)

            # 데이터 PostgreSQL에 저장
            df.to_sql('clima_ml_weather_data', engine, if_exists='append', index=False)
            self.log.info(f"[INFO] Inserted data into DB for range {current_start.format('YYYY-MM-DD')} to {end_of_period.format('YYYY-MM-DD')}.")

            # 다음 날짜로 이동
            current_start = end_of_period.add(days=1)

        # 작업 완료 로그
        self.log.info(f"[INFO] Completed processing data from {start_date.format('YYYY-MM-DD')} to {end_date.format('YYYY-MM-DD')}.")
