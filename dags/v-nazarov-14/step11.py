from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

with DAG(
        'step10nazarov',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        # Описание DAG (не тасок, а самого DAG)
        description='step10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['nazarov10'],
) as dag:

    def get_variable():
        from airflow.models import Variable
        is_prod = Variable.get("is_startml")
        print(is_prod)

    t1 = PythonOperator(
        task_id='get_variable',
        python_callable=get_variable
    )
