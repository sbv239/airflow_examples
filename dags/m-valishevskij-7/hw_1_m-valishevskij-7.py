from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
        'hw_1_m-valishevskij-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 21),
        catchup=False,
        tags=['valishevskij']
) as dag:
    t1 = BashOperator(
        task_id='hw_1_m-valishevskij-7_1',
        bash_command='pwd'
    )


    def print_ds(ds):
        print(ds)
        return 'my return'


    t2 = PythonOperator(
        task_id='hw_1_m-valishevskij-7_2',
        python_callable=print_ds
    )

    t1 >> t2
