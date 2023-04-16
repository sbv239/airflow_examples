# from datetime import datetime,timedelta
# from airflow import DAG 
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from textwrap import dedent

# def branch_func():
#     is_startml = Variable.get("is_startml")
#     if is_startml == "True":
#         return 'startml_desc'
#     else:
#         return 'not_startml_desc'
    
# def start_ml_true():
#     print("StartML is a starter course for ambitious people")

# def not_start_ml():
#     print("Not a startML course, sorry")
 
# with DAG(
#     'new_dag_makararena',
#     default_args={
#     'depends_on_past': False,
#     'email': ['airflow@example.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
# },  
#     tags = ['makararena_tag'],
#     start_date=datetime(2023,4,15)  
# ) as dag:
#     for i in range(10):
#         t1 = BashOperator(
#             task_id = "igor" + '' +str(i),
#             bash_command="echo $NUMBER",
#             env = {"NUMBER": i}
#         )

#     def new_tasks(task_number,ts,run_id):
#         print(run_id)
#         print(ts)
#         return print("task number is:" + ' ' + '{' + str(task_number) + '}')    
        
#     for task in range(20):
#         t2 = PythonOperator(
#             task_id  = f"vitali_{str(task)}",
#             python_callable = new_tasks, 
#             op_kwargs = {'task_number' : task}
#         )
#     t1 >> t2



from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

def branch_func():
    is_startml = Variable.get("is_startml")
    if is_startml == "True":
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def start_ml_true():
    print("StartML is a starter course for ambitious people")

def not_start_ml():
    print("Not a startML course, sorry")

with DAG(
    "ti89234809",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="DAG_test_BranchingOperator",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 19),
    catchup=False,
    tags=["ti-www"],
) as dag:

    t1 = PythonOperator(
        task_id="startml_desc",
        python_callable=start_ml_true,
    )

    t2 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=not_start_ml,
    )

    t3 = BranchPythonOperator(
        task_id='determine_course',
        provide_context=True,
        python_callable=branch_func,
    )

    t4 = DummyOperator(
        task_id="before_branching",
    )

    t5 = DummyOperator(
        task_id="after_branching",
    )

    t4 >> t3 >> [t1, t2] >> t5