from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

gl_var = Variable.get("is_startml")

with DAG(
        "hw_12_s-kim",
        description="Homework 12",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 8, 10),
        catchup=True,
        tags=["s-kim"],
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
) as dag:

    def print_var():
        print(gl_var)

    t1 = PythonOperator(task_id="print_gl_var",
                        python_callable=print_var)

    t1
