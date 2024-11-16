import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



with DAG(
    dag_id="dag_python_with_postgres_hook_bulk_load",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def insrt_postgres(postgres_conn_id, table_name, file_name, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(table_name, file_name)


    insrt_postgres = PythonOperator(
        task_id = 'insrt_postgres',
        python_callable= insrt_postgres,
        op_kwargs= {
            'postgres_conn_id': 'conn-db-postgres-custom',
            'table_name': 'TbListRainfallService_bulk1', # 테이블이 없는 상태에서도 테이블을 생성하고 insert를 하는가?
            'file_name': '/opt/airflow/files/ListRainfallService/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/ListRainfallService.csv'
        }
    )    