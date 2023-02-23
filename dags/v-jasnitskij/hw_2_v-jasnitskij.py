from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'tutorial',
    dafault_args={
        'task_id',
        'bash_command',
    }

) as dag:
    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='MyDAG_1',
        bash_command = 'pwd',
    )

    t2 = PythonOperator(
        task_id='MyDAG_2',
        python_callable=print_context,
        #op_kwargs={'ds': ds, 'kwargs':kwargs}
    )

    t1 >> t2

def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

run_this = PythonOperator(
    task_id='print_the_context',  # нужен task_id, как и всем операторам
    python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
)

