from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator

def changing_branches():
    from airflow.models import Variable
    
    if Variable.get("is_startml") == True:
        return "startml_desc"
    else:
        return "not_startml_desc"
    
def greetings_startML():
    print("StartML is a starter course for ambitious people")
    
def noML_here():
    print("Not a startML course, sorry")
              
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
        
        dummy_1 = DummyOperator(
            task_id = 'before_branching'
        )
        
        BranchPO = BranchPythonOperator(
            task_id = 'determine_course',
            python_callable = changing_branches
            
        )
        
        Py_operator_1 = PythonOperator(
            task_id = 'startml_desc',
            python_callable = greetings_startML
        )
        
        Py_operator_2 = PythonOperator(
            task_id = 'not_startml_desc',
            python_callable = noML_here
        )
        
        dummy_2 = DummyOperator(
            task_id = 'after_branching'
        )
        
        dummy_1 >> BranchPO >> [Py_operator_1,Py_operator_2] >> dummy_2