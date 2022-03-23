from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def get_variable():
    from airflow.models import Variable

    result = Variable.get('is_startml')
    print(result)


with DAG(
        'dag_11_m-tsaj',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Simple variable dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 20),
        catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='print_variable',
        python_callable=get_variable,
    )
