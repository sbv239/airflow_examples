from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


def get_var():
    print(Variable.get("is_startml"))


with DAG(
    'hw_e-zastrogina_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 15)
) as dag:
    t1 = PythonOperator(
        task_id='get_var',
        python_callable=get_var,
        provide_context=True
    )
