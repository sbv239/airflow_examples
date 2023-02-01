from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def push_xcom(ti):
    return 'Airflow tracks everything'

def pull_xcom(ti):
    print(
        ti.xcom_pull(
            key='return_value',
            task_ids='XCom_push_with_Python'
        )
    )


with DAG(
    'r-kutuzov-1_dag_9-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Airflow lesson step 9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 30),
    catchup=False,
    tags=['r-kutuzov-1_step_9'],
) as dag:
    
    t1 = PythonOperator(
        task_id=f'XCom_push_with_Python', 
        python_callable=push_xcom,
    )
    
    t2 = PythonOperator(
        task_id=f'XCom_pull_with_Python', 
        python_callable=pull_xcom,
    )

    t1 >> t2
