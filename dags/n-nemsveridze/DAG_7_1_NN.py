from airflow import DAG

from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'task7_1NN',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='ten times bash twenty times py',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 21),
        catchup=False,
        tags=['NNtask7_1'],
) as dag:

    def cycle_twenty(task_number, ts,run_id,**kwargs):
        return print(f'{task_number},{ts},{run_id}', sep='\n')


    for j in range(20):
        task1 = PythonOperator(task_id='task_number_' + str(j),
                               python_callable=cycle_twenty,
                               op_kwargs={'task_number': int(j), 'ts': '{{ts}}', 'run_id': '{{run_id}}'},
                               dag=dag)
