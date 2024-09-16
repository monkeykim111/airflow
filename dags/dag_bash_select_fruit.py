from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dag_bash_select_fruit",
    schedule="10 0 * * 6#1",  # 첫째주 토요일 매 0시 10분에 실행
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
    )

    t2_avocado = BashOperator(
    task_id="t2_avocado",
    bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )

    t1_orange >> t2_avocado