from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_d-shestak_2',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='hw_d-shestak_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 21),
        catchup=False,
        tags=['task_1'],
) as dag:

    templated_command = dedent(
        """
        {% for i in range(10) %}
            echo i
        """
    )
    for i in range(10):
        task = BashOperator(
            task_id='task_' + str(i),
            bash_command=f"echo task_{i}_by_Bash"
        )

    def print_smth(text):
        print(text)

    for i in range(11, 30):
        task = PythonOperator(
            task_id='task_' + str(i),
            python_callable=print_smth,
            op_kwargs={'text': 'task ' + str(i) + 'by Python'}
        )



