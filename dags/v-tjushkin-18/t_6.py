from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'v-tjushkin-18_t6',
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Lesson 11 (Task 6)',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 16),
        catchup=False,
) as dag:

    def print_task_id(ts, run_id, task_number):
        print(f"ts is: {ts}")
        print(f"run_id is: {run_id}")
        print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id=f"t6_python_{i}",
            python_callable=print_task_id,
            op_kwargs={'task_number': i}
        )

    t2