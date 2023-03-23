from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    "ti-www_task7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="more arguments in Python Operator",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 19),
    catchup=False,
    tags=["ti-www"],
) as dag:

   
    def print_ts_run_id(ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        print(kwargs)

    for i in range(10, 30):
        task_number = i
        t = PythonOperator(
            task_id="task_" + str(i),
            python_callable=print_ts_run_id,

            op_kwargs={'task_number': i},
        )

    t