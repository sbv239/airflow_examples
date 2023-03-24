from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG(
        'hw_3_m-gromov-18',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for unit 2',
        tags=['DAG-3_m-gromov-18'],
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 23),

) as dag:
    def task_number_return(task_number):
        return f"task number is: {task_number}"
    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id='task_number_' + str(i),
                bash_command=f"echo {i}"
            )
        else:
            task = PythonOperator(
                task_id='task_number_' + str(i),
                python_callable=task_number_return,
                op_kwargs={'task_number': int(i)}
            )