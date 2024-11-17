from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from common.climaml_data_utils import fetch_weather_data
import pendulum
import pandas as pd
import numpy as np

class ClimamlFetchHistoricalWeatherDataOperator(BaseOperator):
    def __init__(self, conn_id, end_date, station_ids, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.end_date = pendulum.parse(end_date)  # pendulum으로 end_date를 처리
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
            current_start = pendulum.parse(last_processed_date).add(days=1)  # pendulum으로 변환 후 1일 추가
        else:
            current_start = pendulum.datetime(2014, 11, 17, tz='Asia/Seoul')  # 기본값 설정 (pendulum)

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
                'numOfRows': '900',
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
            df.replace("", np.nan, inplace=True)
            df.to_sql('weather_data', engine, if_exists='append', index=False)

            self.log.info(f"[INFO] Inserted data into DB for range {current_start.format('YYYY-MM-DD')} to {end_of_period.format('YYYY-MM-DD')}.")
            current_start = end_of_period.add(days=1)  # 다음 구간 시작 날짜로 이동

        # variable에 last_processed_date을 저장
        Variable.set("last_processed_date", current_start.format('YYYY-MM-DD'))  # pendulum으로 포맷 후 저장
        print(f"[INFO] Updated last_processed_date to {current_start.format('YYYY-MM-DD')}")
