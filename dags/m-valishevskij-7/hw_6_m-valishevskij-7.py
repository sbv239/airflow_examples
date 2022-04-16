from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
        'hw_6_m-valishevskij-7',
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
    for task_number in range(1, 11):
        task = BashOperator(
            task_id=f'hw_6_m-valishevskij-7_{task_number}',
            bash_command=f"echo {task_number}"
        )

    def python_func(task_number, ts, run_id, **kwargs):
        print(f'task number is : {task_number}')
        print(ts)
        print(run_id)
        kwargs['task_number'] = task_number
        return 'I did everything'

    for task_number in range(11, 31):
        task = PythonOperator(
            task_id=f'hw_6_m-valishevskij-7_{task_number}',
            python_callable=python_func(),
            op_kwargs={'task_number': task_number}
        )

