from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime


with DAG(
        'hw_11_m-valishevskij-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 14),
        catchup=False,
        tags=['valishevskij']
) as dag:
    def get_variable():
        from airflow.models import Variable

        print(Variable.get("is_startml"))
        return 'done'

    t1 = PythonOperator(
        task_id='hw_11_m-valishevskij-7_1',
        python_callable=get_variable
    )


