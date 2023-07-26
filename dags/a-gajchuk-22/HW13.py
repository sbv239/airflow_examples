from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG
from datetime import timedelta, datetime
from airflow.models import Variable


with DAG('hw_a-gajchuk-22_13',
         default_args={
                        'depends_on_past': False,
                        'email': ['airflow@example.com'],
                        'email_on_failure': False,
                        'email_on_retry': False,
                        'retries': 1,
                        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
                    },
         description = 'Dag for hw13',
         start_date = datetime(2023, 7, 25),
         tags = ["agaychuk13"]) as dag:

    def determine():

        is_startml = Variable.get("is_startml")

        if is_startml == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"

    def print1():
        print("StartML is a starter course for ambitious people")

    def print2():
        print("Not a startML course, sorry")

    t1 = PythonOperator(task_id = "startml_desc",
                        python_callable = print1)

    t2 = PythonOperator(task_id = "not_startml_desc",
                        python_callable = print2)

    t3 = BranchPythonOperator(task_id = "determine_course",
                              python_callable = determine)

        


    t3>>[t1,t2]
    
