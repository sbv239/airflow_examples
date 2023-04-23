from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'les_11_task_2_i-osiashvili-19',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 4, 22),
        schedule_interval=timedelta(days=1),
) as dag:
    task_1 = BashOperator(
        task_id="show_directory",
        bash_command='pwd'
    )


    def return_date(ds):
        print("One strange thing, when I type DAG, I'm always typing GAD )))")
        return ds


    task_2 = PythonOperator(
        task_id='date_and_text',
        python_callable=return_date,
    )

    task_1 >> task_2