from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator


from datetime import timedelta,datetime


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def is_ml_desc():
    from airflow.models import Variable
    result = Variable.get("is_startml")
    if result == 'True':
        return "startml_desc"
    else:
        return "not_startml_desc"

def ml_desc():
    print("StartML is a starter course for ambitious people")

def ml_not_desc():
    print("Not a startML course, sorry")


with DAG(
'il_shestov_task_12',
default_args = default_args,
schedule_interval= timedelta(days=1),
start_date = datetime(2023,4,12),
catchup = False
) as dag:
    a1 = DummyOperator(
        task_id='dummy_start'
    )
    t1 = BranchPythonOperator(
        task_id="Branch_op",
        python_callable=is_ml_desc)

    t2 = PythonOperator(
        task_id = "startml_desc",
        python_callable=ml_desc
    )

    t3 = PythonOperator(
        task_id = "not_startml_desc",
        python_callable = ml_not_desc
    )
    a2 = DummyOperator(
        task_id='dummy_finish'
    )

 a1 >> t1 >> [t2, t3] >> a2