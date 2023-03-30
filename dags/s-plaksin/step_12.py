from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator



def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)

with DAG(
        'hw_12_s-plaksin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Implicit Xcom pull and push',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 28),
        catchup=False,
        tags=['hw_12'],
) as dag:
    push_task = PythonOperator(
        task_id='push',
        python_callable=get_variable
    )
