from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'hw_3_m-korablin',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='my_first_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['VanDarkholme'],
) as dag:

    for i in range(10):
        tB = BashOperator(
            task_id='tB'+str(i),
            bash_command=f"echo {i}"
        )
    


    def power2(arg):
        """Возведение в квадрат"""
        print(f"Возвожу в квадрат {arg['task_number']}")
        return arg**2

    for i in range(20):
        tP = PythonOperator(
            task_id='tP'+str(i),
            python_callable=power2,
            op_kwargs={'task_number': i}
        )
    tB >> tP
    