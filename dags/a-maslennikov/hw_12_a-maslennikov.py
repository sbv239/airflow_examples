import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
    'hw_12_a-maslennikov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = "Making DAG for 12th task",
    schedule_interval = datetime.timedelta(days=1),
    start_date = datetime.datetime(2023, 5, 26),
    catchup = False,
    tags = ["hw_12_a-maslennikov"],
) as dag:

    def print_var():
        a = Variable.get("is_startml")
        print(a)

    t1 = PythonOperator(
        task_id = "print_var",
        python_callable = print_var,
    )

    t1
