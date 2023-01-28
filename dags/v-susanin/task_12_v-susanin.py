from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

def get_var():
    from airflow.models import Variable
    result = Variable.get("is_startml")
    print(result)

with DAG(
        'v-susanin_task_12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='v-susanin_DAG_task_12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 28),
        catchup=False,
        tags=['DAG_task_12'],
) as dag:
    first = PythonOperator(
        task_id="Print_get",
        python_callable=get_var
    )
