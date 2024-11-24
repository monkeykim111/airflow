from airflow.sensors.base import BaseSensorOperator
import requests

class ClimamlDataSensor(BaseSensorOperator):
    template_fields = ('api_key', 'check_date') # 템플릿 필드 등록

    def __init__(self, check_date, url, api_key, station_ids, **kwargs):
        super().__init__(**kwargs)
        self.check_date = check_date
        self.url = url
        self.api_key = api_key
        self.station_ids = station_ids

    def poke(self, context):
        # check_date 날짜로 api요청이 가능한지 확인하고 true, false 리턴
        params = {
            'serviceKey': self.api_key,
            'pageNo': '1',
            'numOfRows': '1',
            'dataType': 'JSON',
            'dataCd': 'ASOS',
            'dateCd': 'DAY',
            'startDt': self.check_date,
            'endDt': self.check_date,
            'stnIds': self.station_ids[0]  # 첫뻔째 관측소만 테스트
        }

        try:
            response = requests.get(self.url, params=params)
            response.raise_for_status()  # HTTP 상태 코드 확인 -> HTTP 상태 코드가 4xx 또는 5xx일 경우 예외를 발생
            data = response.json()

            result_code = data.get('response', {}).get('header', {}).get('resultCode')
            if result_code != "00":
                self.log.error(f'[ERROR] API 요청 실패: {result_code} - {data.get("response", {}).get("header", {}).get("resultMsg")}')
                return False

            items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
            if items:
                self.log.info(f"[INFO] Data available for date: {self.check_date}")
                return True
            else:
                self.log.warning(f"[WARNING] No data for date: {self.check_date}")
                return False
        
        except requests.exceptions.RequestException as e:
            self.log.error(f"[ERROR] HTTP 요청 실패: {e}")
            return False
        except ValueError as e:
            self.log.error(f"[ERROR] JSON 디코드 실패: {e}")
            return False
        except Exception as e:
            self.log.error(f"[ERROR] 예상치 못한 오류 발생: {e}")
            return False