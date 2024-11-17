import requests
import pandas as pd
from common.climaml_column_mapping import SELECTED_COLUMNS

def fetch_weather_data(params_base, station_ids, url):
    all_data = []  # 모든 데이터를 저장할 리스트

    for station_id in station_ids:
        params = params_base.copy()
        params['stnIds'] = station_id
        page = 1  # 페이지 번호 초기화

        while True:
            # 페이징 처리
            params['pageNo'] = str(page)
            params['numOfRows'] = '999'  # 요청 건수를 999로 제한

            # API 요청
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"[ERROR] Failed to fetch data for station {station_id}. Status code: {response.status_code}")
                print(f"[ERROR] Response content: {response.text}")
                break

            # 응답 데이터 파싱
            data = response.json()
            if 'response' not in data or 'body' not in data['response']:
                print(f"[ERROR] Missing 'body' in API response for station {station_id}. Response: {data}")
                break

            # 데이터 아이템 가져오기
            items = data['response']['body'].get('items', {}).get('item', [])
            if not items:
                print(f"[INFO] No more data for station {station_id}. Stopping pagination.")
                break

            # 필요한 컬럼만 선택하여 필터링
            filtered_data = [
                {new_key: item.get(old_key, None) for old_key, new_key in SELECTED_COLUMNS.items()}
                for item in items
            ]
            all_data.extend(filtered_data)  # 결과 리스트에 추가

            # 다음 페이지로 이동
            print(f"[INFO] Fetched page {page} for station {station_id}.")
            page += 1

    return pd.DataFrame(all_data)  # Pandas DataFrame으로 반환
