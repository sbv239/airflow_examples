from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'v-susanin_task_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='v-susanin_DAG_task_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 26),
    catchup=False,
    tags=['DAG_task_7'],

) as dag:
    def get_task_number(ts, run_id, **kwargs):
        print(ts)
        print(run_id)

    for i in range(20):
        second=PythonOperator(
            task_id='task_number'+str(i),
            python_callable=get_task_number,
            op_kwargs={'task_number':i})



