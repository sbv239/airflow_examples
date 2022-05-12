from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


with DAG(
    "task_11",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": days_ago(2),
    },
    catchup=False,
) as dag:

    def var_printer():
        print(Variable.get("is_startml"))

    PythonOperator(
        task_id="print_var",
        python_callable=var_printer,
        dag=dag,
    )
