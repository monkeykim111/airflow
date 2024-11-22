from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.hooks.base import BaseHook
import pendulum


with DAG(
    dag_id='dag_file_sensor',
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 11, 15, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def check_api_update(http_conn_id, endpoint, base_dt_col, **kwagrs):
        import requests
        import json
        
        connection = BaseHook.get_connection(http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{endpoint}/1/100'
        response = requests.get(url)

        contents = json.load(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        last_dt = row_data[0].get(base_dt_col)
        last_date = last_dt[:10]
        last_date = last_date.replace('.', '-').replace('/', '-')
        
        try:
            pendulum.from_format(last_date, 'YYYY-MM-DD')
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f'{base_dt_col} 컬럼은 YYYY.MM.DD 또는 YYYY/MM/DD 형태가 아닙니다')

        today_ymd = kwagrs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y-%m-%d')

        if last_date >= today_ymd:
            print(f'생성 확인(배치 날짜: {today_ymd} / API Last 날짜: {last_date})')
            return True
        else:
            print(f'Update 미완료 (배치 날짜: {today_ymd} / API Last 날짜: {last_date})')
            return False

    sensor_task = PythonSensor(
        task_id = 'sensor_task',
        python_callable=check_api_update,
        op_kwargs={
            'http_conn_id': 'openapi.seoul.go.kr',
            'endpoint': '{{var.value.apikey_openapi_seoul_go.kr}}/json/TbCorona19CountStatus',
            'base_dt_col': 'S_DT'
        },
        poke_interval=600,
        mode='reschedule' 
    )
