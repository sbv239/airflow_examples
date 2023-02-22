from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'tutorial'
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

"""
# Генерируем таски в цикле - так тоже можно
for i in range(5):
    # Каждый таск будет спать некое количество секунд
    task = PythonOperator(
        task_id=''
        python_callable=print_context,
        # передаем в аргумент с названием random_base значение float(i) / 10
        op_kwargs={'random_base': float(i) / 10},
"""