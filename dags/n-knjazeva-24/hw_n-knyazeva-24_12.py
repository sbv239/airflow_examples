from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
        'hw_n-knjazeva-24_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 9, 21),
        schedule_interval=timedelta(days=1)
) as dag:
    def get_variable():
        from airflow.models import Variable
        is_startml = Variable.get("is_startml")
        print(is_startml)


    t1 = PythonOperator(
        task_id='get_var',
        python_callable=get_variable,
    )
