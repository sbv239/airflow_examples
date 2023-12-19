from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def var_push(ti):
    ti.xcom_push(key="sample_xcom_key", value="xcom test")


def var_pull(ti):
    result = ti.xcom_pull(key="sample_xcom_key", task_ids='task_push')
    print(result)


with DAG('hw-b-shramko-8',
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
        task_id='task_push',
        python_callable=var_push
    )
    t2 = PythonOperator(
        task_id='task_pull',
        python_callable=var_pull
    )

    t1 >> t2
