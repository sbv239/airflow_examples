from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
        's_pletnev_task_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_3_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 23),
        catchup=False,
        tags=['task_3'],
) as dag:
    def print_task_number(**kwargs):
        print(f"task number is: {kwargs['task_number']}")
        return "task number printed"


    for i in range(30):
        task_1 = BashOperator(
            task_id="echo_task_number",
            bash_command=f"echo {i}"
        )
        if i >= 10:
            task_2 = PythonOperator(
                task_id='print_task_numbe: ' + str(i),  # в id можно делать все, что разрешают строки в python
                python_callable=print_task_number,
                op_kwargs={'task_number': i},
            )

            task_1 >> task_2
