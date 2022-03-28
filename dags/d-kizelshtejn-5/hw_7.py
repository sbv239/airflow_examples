from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_tn_ts_run_id(task_number, ts, run_id):
    print(f"task_number is: {task_number}")
    print(ts)
    print(run_id)


with DAG(
        'hw_7_d-kizelshtejn-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for hw_7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 27),
        catchup=False,
        tags=['hw_7']
) as dag:

    for i in range(10, 30):
        t1 = PythonOperator(
            task_id='print_tn_ts_run_id_' + str(i),
            python_callable=print_tn_ts_run_id,
            op_kwargs={'task_number': i}
        )

    t1.doc_md = dedent(
        """
        ## Создаем __DAG__ _**типа `PythonOperator`**_
        передать в _**kwags**_ __`task_number`__ со значением цикла, 
        также добавить прием аргрументов `ts` и `run_id` в функции и распечатать эти значения
        """
    )

    t1
