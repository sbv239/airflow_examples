from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG('hw_3_m-zharmakin-6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_3_m-zharmakin-6']
        ) as dag:
    
    for i in range(10):
        bash_task = BashOperator(
            task_id=f'bash_task_{i}',
            env={"NUMBER": i},
            bash_command="echo $NUMBER"
        )
        
    def py_task(ts, run_id, task_number, **kwargs):
        print(ts)
        print(run_id)
        print('task number is: {task_number}')
        return 'pytask'
        
    for i in range(20):
        python_task = PythonOperator(
            task_id=f'python_task_{i}',
            python_callable=py_task,
            op_kwargs={"task_number": i}
        )
        
    bash_task >> python_task
