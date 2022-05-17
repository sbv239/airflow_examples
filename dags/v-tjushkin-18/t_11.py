from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'v-tjushkin-18_t11',
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Lesson 11 (Task 11)',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 16),
        catchup=False,
) as dag:

    def get_variable():
        from airflow.models import Variable

        print(Variable.get("is_startml"))

    t1 = PythonOperator(
        task_id="t11_python_1",
        python_callable=get_variable,
    )

    t1