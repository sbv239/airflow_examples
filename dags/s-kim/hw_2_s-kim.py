from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        "hw_2_s-kim",
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
) as dag:
    # Python function to print logic's date
    def python_func(ds, **kwargs):
        print(ds)
        print("End of execution of Python Operator")

    # Task 1 to print working directory
    t1 = BashOperator(
        task_id="bash_01",
        bash_command="pwd"
    )

    # Task 2 to print logic's date
    t2 = PythonOperator(
        task_id="python_01",
        python_callable=python_func
    )

    # Sequence of tasks
    t1 >> t2