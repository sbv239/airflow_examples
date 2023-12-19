from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def return_print(ti):
    return "Airflow tracks everything"


def var_pull(ti):
    result = ti.xcom_pull(key="return_value", task_ids='task_return_print')
    print(result)


with DAG('hw-b-shramko-9',
         default_args={
             'depends_on_past': False,
             'email': ['airflow@example.com'],
             'email_on_failure': False,
             'email_on_retry': False,
             'retries': 1,
             'retry_delay': timedelta(minutes=5),
         },
         start_date=datetime(2022, 1, 1),
         catchup=False
         ) as dag:
    t1 = PythonOperator(
        task_id='task_return_print',
        python_callable=return_print
    )
    t2 = PythonOperator(
        task_id='task_pull',
        python_callable=var_pull
    )

    t1 >> t2
