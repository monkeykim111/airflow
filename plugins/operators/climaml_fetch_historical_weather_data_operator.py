from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from common.climaml_data_utils import fetch_weather_data
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

class ClimamlFetchHistoricalWeatherDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, conn_id, end_date, station_ids, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.end_date = end_date
        self.station_ids = station_ids

    def execute(self, context):
        # postgres hook 설정
        postgres_hook = PostgresHook(postgres_conn_id=self.conn_id)
        api_key = Variable.get("data_go_kr")  # variable에서 data_go_kr의 value가져옴
        url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'  # 요청할 url

        # variable에 last_processed_date 변수가 있는지 확인, 없으면 default_var가 None이므로 그것을 반환
        # 일일 트래픽이 10000건 이므로, 여러 관측소에서 10년치의 데이터를 요청해야 하는 상황이므로
        # 일일 트래픽이 넘은 경우, 다음날 저장된 date에서부터 다시 요청을 진행하기 위한 변수
        last_processed_date = Variable.get('last_processed_date', default_var=None)
        if last_processed_date:
            current_start = datetime.strptime(last_processed_date, '%Y-%m-%d') + timedelta(days=1)
        else:
            current_start = datetime.strptime('2014-11-17', '%Y-%m-%d')
        current_end = datetime.strptime(self.end_date, '%Y-%m-%d')

        engine = postgres_hook.get_sqlalchemy_engine()
        request_count = 0
        max_requests_per_day = 10000

        while current_start <= current_end:
            if request_count >= max_requests_per_day:
                self.log.info("일일 최대 API request를 하였습니다. 동작을 멈춥니다...")
                break

            end_of_period = min(current_start - timedelta(days=90), current_end)

            # 요청할 파라미터
            params_base = {
                'serviceKey': api_key,
                'pageNo': '1',
                'numOfRows': '1000',
                'dataType': 'JSON',
                'dataCd': 'ASOS',
                'dateCd': 'DAY',
                'startDt': current_start.strftime('%Y%m%d'),
                'endDt': end_of_period.strftime('%Y%m%d')
            }

            # API 요청
            df = fetch_weather_data(params_base=params_base, station_ids=self.station_ids, url=url)
            request_count += len(self.station_ids)

            self.log.info(f"[INFO] Processing data from {current_start.strftime('%Y-%m-%d')} to {end_of_period.strftime('%Y-%m-%d')}.")
            df.replace("", np.nan, inplace=True)
            df.to_sql('weather_data', engine, if_exists='append', index=False)

            self.log.info(f"[INFO] Inserted data into DB for range {current_start.strftime('%Y-%m-%d')} to {end_of_period.strftime('%Y-%m-%d')}.")
            current_start = end_of_period + timedelta(days=1)

        # variable에 last_processed_date을 저장
        Variable.set("last_processed_date", current_start.strftime('%Y-%m-%d'))
        print(f"[INFO] Updated last_processed_date to {current_start.strftime('%Y-%m-%d')}")



















