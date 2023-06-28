from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
import time

with DAG(
    'hw_3_m-sazonov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='hw_2_m-sazonov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 27),
    catchup=False,
) as dag:

    templated_command = dedent(
        """
    {% for i in range(10) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    t1 = BashOperator(
        task_id='hw_3_m-sazonov_print_ds',
        bash_command=templated_command
    )


    def my_sleeping_function(random_base):
        time.sleep(random_base)

    for i in range(10,31):
        t2 = PythonOperator(
            task_id='task number is: ' + str(i),
            python_callable=my_sleeping_function,
            op_kwargs={'random_base': float(i) / 10},
        )

    t1 >> t2