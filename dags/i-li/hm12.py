from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def get_sql():
    from airflow.models import Variable
    is_prod = Variable.get('is_startml')
    print(is_prod)


with DAG(
        'hm_12_i-li',
        default_args={
            'dependes_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_in_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        start_date=datetime(2023, 2, 15)
) as dag:
    t1 = PythonOperator(
        task_id='hm_12_i-li',
        python_callable=get_sql
    )
