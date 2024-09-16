import datetime
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_bash_operator",  # dag_id = dag 이름 / 파이썬 파일 명과 일치 시키기
    schedule="0 0 * * *",  # 분 시 일 월 요일
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),  # 언제부터 돌 것인가 / UTC 세계 표준 시 Asia/Seoul 로 변경
    catchup=False,  # 현재 날짜와 start date 사이의 간극만큼을 소급 적용하여 돌릴 것인가? 
    # dagrun_timeout=datetime.timedelta(minutes=60),  # 타임아웃 값 
    tags=["example", "example2"],  # 태그 / 옵셔널함
    # params={"example_key": "example_value"},  # 태스크들을 선언할 때 공통적으로 넘겨줄 파라미터를 적음
) as dag:
    # [START howto_operator_bash]
    bash_task1 = BashOperator(
        task_id="bash_task1",  # graph에 나오는 이름 / 객체명과 태스크 아이디는 동일하게 주자
        bash_command="echo whoami",  # 어떤 쉘 스크립트를 수행할 것인가?
    )
    # [END howto_operator_bash]

    # [START howto_operator_bash]
    bash_task2 = BashOperator(
        task_id="bash_task2",  # graph에 나오는 이름 / 객체명과 태스크 아이디는 동일하게 주자
        bash_command="echo $HOSTNAME",  # 어떤 쉘 스크립트를 수행할 것인가?
    )
    # [END howto_operator_bash]

    bash_task1 >> bash_task2