import requests
import pandas as pd
from common.climaml_column_mapping import SELECTED_COLUMNS


def fetch_weather_data(params_base, station_ids, url):
    all_data = []
    for station_id in station_ids:
        params = params_base.copy()
        params['stnIds'] = station_id
        response = requests.get(url, params=params)

        if response.status_code == 200:  # 요청 성공
            print(f"[INFO] Successfully fetched data for station {station_id}.")
            try:
                data = response.json()  # JSON 변환 시도
            except ValueError as e:
                print(f"[ERROR] Failed to parse JSON response for station {station_id}. Error: {e}")
                print(f"[DEBUG] Response content: {response.text[:500]}")  # 응답 내용 일부 출력
                continue  # 다음 관측소로 넘어감
            
            # 데이터 추출 및 필터링
            items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
            if not items:
                print(f"[INFO] No data found for station {station_id}.")
                continue

            filtered_data = [
                {new_key: item.get(old_key, None) for old_key, new_key in SELECTED_COLUMNS.items()}
                for item in items
            ]
            all_data.extend(filtered_data)
        else:  # 요청 실패
            print(f"[ERROR] Failed to fetch data for station {station_id}. Status code: {response.status_code}")
            print(f"[DEBUG] Response text: {response.text[:500]}")
            continue

    return pd.DataFrame(all_data)