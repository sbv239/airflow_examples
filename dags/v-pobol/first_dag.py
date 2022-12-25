from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
with DAG(
        'pobol_first_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date = datetime(2022, 12, 24),
        schedule_interval = timedelta(days=1)
        ) as dag:
    

    def print_s(task_number, ts, run_id):
        print(task_number, ts, run_id)

    for i in range(30):

        if i < 10:
            task = BashOperator(
                    task_id = 'bash_task_' + str(i),
                    bash_command = 'echo $NUMBER',
                    dag=dag,
                    env = {'NUMBER': str(i)} 
                    )
            task.doc_md = """
            `This`
            _my_
            __first__
            # dag
            """
        else:
            task = PythonOperator(
                    task_id = 'python_task_' + str(i),
                    python_callable = print_s,
                    op_kwargs = {"task_number":i, 'ts': "{{ ts }}", 'run_id': "{{ run_id }}"}
                    )
            task.doc_md = """
            `This`
            _my_
            __first__
            # dag
            """
