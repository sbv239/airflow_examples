from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable


with DAG(
        'hw_13_a-tjurin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='Task 13',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 19),
        catchup=False,
        tags=['Task_13'],
) as dag:

    def decide_which_path():
        if Variable.get("is_startml") == 'True':
           return "startml_desc"
        else:
            return "not_startml_desc"

    def print_true():
       print("StartML is a starter course for ambitious people")
       return 'Print text from task_id - startml_desc'

    def print_other():
       print("Not a startML course, sorry")
       return 'Print text from task_id - not_startml_desc'

    t1 = BranchPythonOperator(
        task_id='run_this_first',
        python_callable=decide_which_path,
        trigger_rule="one_success"
    )

    t2 = PythonOperator(
        task_id='startml_desc',
        python_callable=print_true
    )


    t3 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_other
    )

    t1 >> [t2, t3]
