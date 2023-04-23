from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
        'e_10_demets',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['demets'],
) as dag:
    def first_handler(ti):
        return 'Airflow tracks everything'


    def second_handler(ti):
        print(ti.xcom_pull(key='return_value', task_ids='first_task'))


    t1 = PythonOperator(
        task_id='first_task',
        python_callable=first_handler,
        dag=dag
    )

    t2 = PythonOperator(
        task_id='second_task',
        python_callable=second_handler,
        dag=dag
    )

    t1 >> t2
