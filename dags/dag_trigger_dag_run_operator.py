from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="dag_trigger_dag_run_operator",
    schedule="10 9 * * *", 
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start bash operator!"'
    )

    trigger_dag_run_task = TriggerDagRunOperator(
        task_id='trigger_dag_run_task',
        trigger_dag_id='dag_python_operator',  # 실행시킬 dag id
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',  # 현재 dag의 data interval start로 실행시키도록  // trigger run id를 주지 않고 execution date값만 준 것이라 manual로 나옴 // execution_date를 실행시킨 dag의 data_inteval_start로 주었기 때문에 해당 schedule의 값이 나온다.
        reset_dag_run=True,
        wait_for_completion=False,  # trigger 된 dag이 완료될 때까지 기다리지 않음
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
    )

    start_task >> trigger_dag_run_task

