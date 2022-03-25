from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def print_variable():
    from airflow.models import Variable
    print(Variable.get("is_startml"))
    
with DAG\
    (
    "task_12_v_zabolotskij",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description = "DAG for task #12",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 20),
    catchup = False,
    tags = ["task_12"]
    ) as dag:
        
        task_1 = PythonOperator(
            task_id = "variable_call",
            python_callable = print_variable       
        )