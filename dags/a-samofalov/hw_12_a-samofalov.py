from airflow.operators.python import BranchPythonOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'hw_12_a-samofalov',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    def branch_chosing():
        if Variable.get('is_startml') == 'True':
            out = "startml_desc"
        else:
            out = "not_startml_desc"
        return out

    def true_branch():
        print("StartML is a starter course for ambitious people")
        return 'TRUE'

    def false_branch():
        print("Not a startML course, sorry")
        return 'FALSE'

    dag_start = BranchPythonOperator(
        task_id='dag_start',
        python_callable=branch_chosing
    )

    dag_is_true = PythonOperator(
        task_id="startml_desc",
        python_callable=true_branch
    )

    dag_is_false = PythonOperator(
        task_id="not_startml_desc",
        python_callable=false_branch
    )
    dag_start >> [dag_is_true, dag_is_false]
