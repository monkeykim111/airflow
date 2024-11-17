import requests
import pandas as pd
from common.climaml_column_mapping import SELECTED_COLUMNS


def fetch_weather_data(params_base, station_ids, url):
    all_data = []
    for station_id in station_ids:
        params = params_base.copy()
        params['stnIds'] = station_id
        response = requests.get(url, params=params)
        if response.status_code == 200:
            print(f"[INFO] Successfully fetched data for station {station_id}.")
            data = response.json()
            items = data['response']['body']['items']['item']
            filtered_data = [
                {new_key: item.get(old_key, None) for old_key, new_key in SELECTED_COLUMNS.items()}
                for item in items
            ]
            all_data.extend(filtered_data)
        else:
            print(f"[ERROR] Failed to fetch data for station {station_id}. Status code: {response.status_code}")
            continue
    return pd.DataFrame(all_data)


