import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator



with DAG(
    dag_id="dag_simple_http_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def insrt_postgres(ip, port, dbname, user, password, **kwargs):
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=password, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insert 수행'
                sql = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    insrt_postgres = PythonOperator(
        task_id = 'insrt_postgres',
        python_callable = insrt_postgres,
        op_args=['172.28.0.3', '5432', 'kim', 'kim', 'kim']
    )

    insrt_postgres