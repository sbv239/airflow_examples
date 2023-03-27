from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta



with DAG(
        'hw_12_m-gromov-18',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for unit 12',
        tags=['DAG-12_m-gromov-18'],
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 26),

) as dag:
    def var():
        from airflow.models import Variable
        res = Variable.get("is_startml")
        print(res)

    t1 = PythonOperator(
        task_id='get_variable',
        python_callable=var
    )