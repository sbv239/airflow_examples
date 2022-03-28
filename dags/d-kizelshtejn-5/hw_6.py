from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'hw_6_d-kizelshtejn-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for hw_6',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 27),
        catchup=False,
        tags=['hw_6']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='print_number_as' + str(i),
            bash_command=f"echo $NUMBER",
            env={'NUMBER': str(i)}
        )

    t1.doc_md = dedent(
        """
        ## Создаем __DAG__ _**типа `BashOperator`**_
        подбрасываем туда переменную окружения __`NUMBER`__, 
        чье значение будет равно `i` из цикла
        """
    )

    t1
