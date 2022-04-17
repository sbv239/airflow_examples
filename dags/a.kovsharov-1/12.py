from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        }

with DAG(
        'hw_12_a.kovsharov',
        # Параметры по умолчанию для тасок
        default_args=default_args,
    # Описание DAG (не тасок, а самого DAG)
    description='A cycle generated DAG of homework of Lesson 11',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['HW_12', 'a.kovsharov']
) as dag:
    
    
    def get_is_startml():
        from airflow.models import Variable
        return Variable.get("is_startml")
        
        
    def print_start_course():
        print("StartML is a starter course for ambitious people")
   
        
    def print_not_start_course():
       print("Not a startML course, sorry")
       
       
    def select_next_task():
        is_startml = get_is_startml()
        print(f"is_startml - {is_startml}")
        if is_startml == 'True':
            return "startml_desc"
        else:
            return "not_startml_desc"
        
    
    start_op = DummyOperator(
        task_id = "start_dag"
    )
    
    branch_op = BranchPythonOperator(
        task_id = "select_branch",
        python_callable = select_next_task
    )
    
    start_course_op = PythonOperator(
        task_id = "startml_desc",
        python_callable = print_start_course
    )
    
    not_start_course_op = PythonOperator(
        task_id = "not_startml_desc",
        python_callable = print_not_start_course)
    
    end_op = DummyOperator(
        task_id = "end_dag")
    
    start_op >> branch_op >> [start_course_op, not_start_course_op] >> end_op