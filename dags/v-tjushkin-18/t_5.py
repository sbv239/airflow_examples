from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'v-tjushkin-18_t5',
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Lesson 11 (Task 5)',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 16),
        catchup=False,
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f"t5_bash_{i}",
            bash_command=f"echo $NUMBER",
            env={'NUMBER': i}
        )

    t1