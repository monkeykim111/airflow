import requests
import pandas as pd
from common.climaml_column_mapping import SELECTED_COLUMNS


def fetch_weather_data(params_base, station_ids, url):
    all_data = []  # 데이터를 저장할 리스트

    for station_id in station_ids:
        params = params_base.copy()
        params['stnIds'] = station_id
        page = 1  # 페이지 번호 초기화

        while True:
            params['pageNo'] = str(page)
            response = requests.get(url, params=params)

            # 상태 코드 확인
            if response.status_code != 200:
                print(f"[ERROR] Failed request with status code {response.status_code}")
                print(f"[DEBUG] Response text: {response.text}")
                break

            # 데이터 가져오기
            try:
                data = response.json()
            except ValueError as e:
                print(f"[ERROR] Failed to decode JSON for station {station_id}, page {page}: {e}")
                print(response)
                break  # 현재 관측소의 반복 종료, 다음 관측소로 이동

            items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
            if not items:  # 데이터가 없으면 현재 관측소 종료
                print(f"[INFO] No more data for station {station_id} at page {page}. Moving to next station.")
                break

            # 필요한 컬럼만 선택하여 데이터 추가
            filtered_data = [
                {new_key: item.get(old_key, None) for old_key, new_key in SELECTED_COLUMNS.items()}
                for item in items
            ]
            all_data.extend(filtered_data)  # 데이터를 누적

            print(f"[INFO] Successfully fetched page {page} for station {station_id}.")
            page += 1  # 다음 페이지로 이동

    return pd.DataFrame(all_data)  # Pandas DataFrame 반환


