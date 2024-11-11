from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')
    
    def __init__(self, dataset_name, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        # 쉼표 제거하여 tuple이 아닌 문자열로 설정
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_name
        self.base_dt = base_dt

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        print('this is connection:', connection)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000

        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])

            if len(row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000

        # 파일 경로 생성 부분 수정
        directory_path = os.path.dirname(os.path.join(self.path, self.file_name))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        # CSV 파일 저장
        total_row_df.to_csv(os.path.join(self.path, self.file_name), encoding='utf-8', index=False)
    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json

        headers = {
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }

        request_url = f'{base_url}/{start_row}/{end_row}/'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        
        response = requests.get(request_url, headers=headers)
        
        # 응답 확인 후 JSON 파싱
        if response.status_code == 200 and response.text:
            try:
                contents = json.loads(response.text)
            except json.JSONDecodeError:
                self.log.error("Failed to parse JSON response.")
                raise

            key_name = list(contents.keys())[0]
            row_data = contents.get(key_name).get('row')
            row_df = pd.DataFrame(row_data)

            return row_df
        else:
            self.log.error("API response error with status code: %s", response.status_code)
            raise Exception("API response error")
