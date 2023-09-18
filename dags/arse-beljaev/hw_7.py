from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'hw_arse-beljaev_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        },
    description='hw_7_lesson_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 18),
    catchup=False,
    tags=['example']
) as dag:

    def print_conntext(ts, run_id, task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        t1 = PythonOperator(
            task_id='print_number' + str(i),
            python_callable=print_conntext,
            op_kwargs={'task_number': i},
        )

    t1
