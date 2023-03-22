from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator


with DAG(
    'hw_12_m-korablin',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='sqlquery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['VanDarkholme'],
) as dag:

    def startml_desc():
        print("StartML is a starter course for ambitious people")

    def not_startml_desc():
        print("Not a startML course, sorry")

    def turner():
        from airflow.models import Variable

        if Variable.get("is_startml") == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'

    tD1 = DummyOperator(
        task_id = 'Dummy1'
    )

    tD2 = DummyOperator(
        task_id='Dummy2'
    )

    tBPO = BranchPythonOperator(
        task_id='BranchPO',
        python_callable=turner
    )

    tS = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc
    )
    
    tNS= PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc
    )

    tD1 >> tBPO >> [tS, tNS] >> tD2
    