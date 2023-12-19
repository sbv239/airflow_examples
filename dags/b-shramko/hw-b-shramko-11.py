from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_var():
    from airflow.models import Variable

    res = Variable.get("is_startml")
    print(res)


with DAG('hw-b-shramko-11',
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
        task_id='get_var',
        python_callable=get_var
    )
