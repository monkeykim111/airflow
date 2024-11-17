# 공공 데이터 포털에서 사용하는 원본데이터 키를 
# postgresql에 저장할 컬럼 이름으로 변환하기 위한 매핑 변수
# 요청하는 API 키가 변경이 되면 이 부분에서만 변경을 해주면 된다.
# API가 업데이트되어 avgTa라는 키가 averageTemperature로 바뀐다면 SELECTED_COLUMNS에서만 수정하면 됩니다.

SELECTED_COLUMNS = {
    'stnId': 'stn_id',          # 지점 번호
    'tm': 'tm',                 # 일시
    'avgTa': 'avg_ta',          # 평균 기온(°C)
    'minTa': 'min_ta',          # 최저 기온(°C)
    'maxTa': 'max_ta',          # 최고 기온(°C)
    'sumRn': 'sum_rn',          # 일 강수량(mm)
    'avgWs': 'avg_ws',          # 평균 풍속(m/s)
    'avgRhm': 'avg_rhm',        # 평균 상대 습도(%)
    'avgTd': 'avg_td',          # 평균 이슬점 온도(°C)
    'avgPs': 'avg_ps',          # 평균 현지 기압(hPa)
    'ssDur': 'ss_dur'           # 가조 시간(hr)
}