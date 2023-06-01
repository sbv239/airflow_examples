from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from airflow.models import Variable



default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}
with DAG(
    'andre-karasev_hw_12',
    default_args=default_args,
    description='hw_12_',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 9, 9),
    catchup=False,
    tags=['andre-karasev_hw_12']
) as dag:

    def get_task_id():
        is_startml = Variable.get("is_startml")
        if is_startml == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"

    def print_startml_desc():
        print("StartML is a starter course for ambitious people")

    def print_not_startml_desc():
        print("Not a startML course, sorry")

    t1 = DummyOperator(task_id='before_branching')
    t2 = BranchPythonOperator(task_id='determine_course', python_callable=get_task_id)
    t3 = PythonOperator(task_id='not_startml_desc', python_callable=print_not_startml_desc)
    t4 = PythonOperator(task_id='startml_desc', python_callable=print_startml_desc)
    t5 = DummyOperator(task_id='after_branching')

    t1 >> t2 >> [t3, t4] >> t5
