from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    'hw_arse-beljaev_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        },
    description='hw_6_lesson_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 18),
    catchup=False,
    tags=['example']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='print_the_context' + str(i),
            env={'NUMBER': f"{i}"},
            bash_command="echo $NUMBER",
        )

        t1
