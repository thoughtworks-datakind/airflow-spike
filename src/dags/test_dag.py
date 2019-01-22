from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates

default_args = {
    'owner': 'dk_tw',
    'depends_on_past': False,
    'start_date': dates.days_ago(0)
}

test_dag =  DAG("spike", default_args=default_args, max_active_runs=1, catchup=False, schedule_interval=None)

def validate(*args, **kwargs):
    print("validating here")

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=test_dag)

t2 = PythonOperator(
    task_id="validate",
    python_callable=validate,
    provide_context=True,
    dag=test_dag)

t1 >> t2