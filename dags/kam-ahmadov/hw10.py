from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def airflow_tracks(**kwargs):
    return "Airflow tracks everything"

def xcom_realise(ti):
    returned_value = ti.xcom_pull(task_ids='python_track', key='return_value')
    print(f"XCom value received: {returned_value}")

with DAG(
    'hw_kamilahmadov_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
    tags=["hw_10"]
) as dag:
    
    t1 = PythonOperator(
        task_id="python_track",
        python_callable=airflow_tracks
    )

    t2 = PythonOperator(
        task_id="xcom_track",
        python_callable=xcom_realise
    )

    t1 >> t2  # Set the task dependency
