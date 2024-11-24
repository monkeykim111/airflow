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
        self.start_date = pendulum.parse(start_date)  # pendulum으로 start_date를 처리
        self.end_date = pendulum.parse(end_date)  # pendulum으로 end_date를 처리
        self.station_ids = station_ids

    def execute(self, context):
        # postgres hook 설정
        postgres_hook = PostgresHook(postgres_conn_id=self.conn_id)
        api_key = Variable.get("data_go_kr")  # variable에서 data_go_kr의 value 가져옴
        url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'  # 요청할 url

        current_start = self.start_date
        current_end = self.end_date

        engine = postgres_hook.get_sqlalchemy_engine()
        request_count = 0
        max_requests_per_day = 10000

        while current_start <= current_end:
            if request_count >= max_requests_per_day:
                self.log.info("일일 최대 API request를 하였습니다. 동작을 멈춥니다...")
                break

            end_of_period = min(current_start.add(days=90), current_end)  # pendulum의 add()로 90일 추가
            
            # 요청할 파라미터
            params_base = {
                'serviceKey': api_key,
                'pageNo': '1',
                'numOfRows': '999',
                'dataType': 'JSON',
                'dataCd': 'ASOS',
                'dateCd': 'DAY',
                'startDt': current_start.format('YYYYMMDD'),  # pendulum에서 날짜 포맷
                'endDt': end_of_period.format('YYYYMMDD')
            }

            # API 요청
            df = fetch_weather_data(params_base=params_base, station_ids=self.station_ids, url=url)
            request_count += len(self.station_ids)

            self.log.info(f"[INFO] Processing data from {current_start.format('YYYY-MM-DD')} to {end_of_period.format('YYYY-MM-DD')}.")
            # 데이터 중복 제거
            df = df.drop_duplicates()
            df.replace("", np.nan, inplace=True)
            df.to_sql('clima_ml_weather_data', engine, if_exists='append', index=False)

            self.log.info(f"[INFO] Inserted data into DB for range {current_start.format('YYYY-MM-DD')} to {end_of_period.format('YYYY-MM-DD')}.")
            current_start = end_of_period.add(days=1)  # 다음 구간 시작 날짜로 이동

        self.log.info(f"[INFO] Completed processing data from {self.start_date.format('YYYY-MM-DD')} to {self.end_date.format('YYYY-MM-DD')}.")