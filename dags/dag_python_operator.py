import pendulum
import random
from airflow import DAG
from airflow.operators.python import PythonOperator



with DAG(
    dag_id="dag_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 10, 25, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def select_fruit():
        fruit = ['banana', 'orange', 'avocado', 'apple']
        rand_int = random.randint(0, 3)
        print(fruit[rand_int])


    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable=select_fruit
    )

    py_t1

