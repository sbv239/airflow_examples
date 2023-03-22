from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta



with DAG(
    'hw_12_m-korablin',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='sqlquery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['VanDarkholme'],
) as dag:

    def var_test():
        from airflow.models import Variable
        is_prod = Variable.get("is_startml")  # необходимо передать имя, заданное при создании Variable
        print(is_prod)

    task = PythonOperator(
        task_id='variable_test',
        python_callable=var_test
    )


    task
    