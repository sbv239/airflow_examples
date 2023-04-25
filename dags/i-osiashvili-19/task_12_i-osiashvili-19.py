from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator


def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)

with DAG(
        'les_11_task_12_i-osiashvili-19',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 4, 20),
        schedule_interval=timedelta(days=1),

) as dag:
    get_var = PythonOperator(
        task_id="variables_",
        python_callable=get_variable,
    )