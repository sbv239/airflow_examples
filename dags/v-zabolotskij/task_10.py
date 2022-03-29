from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


def return_smth():
    return "Airflow tracks everything"

def get_return(ti):
    
    print(ti.xcom_pull(key = 'return_value',
                       task_ids = 'tracking_function'))    
    

with DAG\
    (
    "task_10_v_zabolotskij",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description = "DAG for task #10",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 20),
    catchup = False,
    tags = ["task_10"]
    ) as dag:
        
        task_1 = PythonOperator(
            task_id = "tracking_function",
            python_callable = return_smth          
            )
        
        task_2 = PythonOperator(
            task_id = "get_tracking_return",
            python_callable = get_return
            )
        
        task_1 >> task_2 