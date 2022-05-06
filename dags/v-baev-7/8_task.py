from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


def put_xcom(ti):
    
    ti.xcom_push(key = 'sample_xcom_key',
                 value = "xcom test")
    
def pull_xcom(ti):
    
    test_xcom = ti.xcom_pull(
                    key = 'sample_xcom_key',
                    task_ids = "put_xcom"           
                    ) 
    print(test_xcom)


with DAG\
    (
    "hw_8_v-baev-7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description = "Testing xcom in airflow",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 20),
    catchup = False,
    tags = ["hw_8"]
    ) as dag:
        
        task_1 = PythonOperator(
            task_id = "put_xcom",
            python_callable = put_xcom,
            )
        task_2 = PythonOperator(
            task_id = "pull_xcom",
            python_callable = pull_xcom,
        )
        
        task_1 >> task_2
        