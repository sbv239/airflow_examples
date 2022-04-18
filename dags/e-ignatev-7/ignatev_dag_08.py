from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    'ignatev_dag_07',
    default_args=default_args,
    start_date=datetime(2022, 4, 15),
    max_active_runs=1,
    schedule_interval=timedelta(days = 1),
) as dag:

    def xcom_return():
        return 'Airflow tracks everything'
        
    def xcom_pull(ti):
        output = ti.xcom_pull(
            key='return_value',
            task_ids='t1_xcom_return',
        )
        print(output)

    t1 = PythonOperator(
        task_id='t1_xcom_return',
        python_callable=xcom_return,
    )

    t2 = PythonOperator(
        task_id='t2_xcom_pull',
        python_callable=xcom_pull,
    )

    t1 >> t2