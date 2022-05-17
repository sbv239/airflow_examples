from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'v-tjushkin-18_t9',
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Lesson 11 (Task 9)',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 16),
        catchup=False,
) as dag:

    def set_variable():
        return "Airflow tracks everything"

    def get_variable(ti):
        ti.xcom_pull(
            key='return_value',
            task_ids='t9_python_1'
        )
        print("variable get")

    t1 = PythonOperator(
        task_id="t9_python_1",
        python_callable=set_variable,
    )

    t2 = PythonOperator(
        task_id="t9_python_2",
        python_callable=get_variable,
    )

    t1 >> t2