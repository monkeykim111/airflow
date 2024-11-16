import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook



with DAG(
    dag_id="dag_python_with_custom_hook_bulk_load",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, table_name, file_name, **kwargs):
        custom_progress_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_progress_hook.bulk_load(table_name=table_name, file_name=file_name, delimiter=',', is_header=True, is_replace=False)
    
    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={
            'postgres_conn_id': 'conn-db-postgres-custom',
            'table_name': 'TbListRainfallService_bulk1',
            'file_name': '/opt/airflow/files/ListRainfallService/ListRainfallService.csv'
        }
    )

    insrt_postgres