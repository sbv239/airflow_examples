from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta



with DAG(
        'hw_13_m-gromov-18',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for unit 13',
        tags=['DAG-13_m-gromov-18'],
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 26),

) as dag:
    def branch_func():
        from airflow.models import Variable
        if Variable.get("is_startml") == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'
    def continue_func():
        return "StartML is a starter course for ambitious people"
    def stop_func():
        return "Not a startML course, sorry"

    before_branching=DummyOperator(
        task_id='before_branching'
    )

    branch_op = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable=branch_func
        )

    continue_op = PythonOperator(
        task_id='startml_desc',
        python_callable=continue_func
        )

    stop_op = PythonOperator(
        task_id='not_startml_desc',
        python_callable=stop_func
        dag=dag)

    after_branching = DummyOperator(
        task_id='after_branching',
        dag=dag
    )

    before_branching >> branch_op >> [continue_op, stop_op] >> after_branching
