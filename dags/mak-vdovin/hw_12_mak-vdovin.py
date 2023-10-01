from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'variable',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='home work "variable"',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False,
    tags=['hw_12'],
) as dag:

    def get_value():
        from airflow.models import Variable

        is_startml = Variable.get("is_startml")
        print(is_startml)
        return is_startml

    PythonOperator(
        task_id='get_value',
        python_callable=get_value,
    )

    if __name__ == '__main__':
        dag.test()