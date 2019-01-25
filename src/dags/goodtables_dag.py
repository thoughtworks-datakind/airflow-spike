from airflow import DAG
from goodtables import Inspector
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
import os
from pprint import pprint
from commons.common_functions import validate_config, validate_mandatory, validate_numeric

default_args = {
    'owner': 'dk_tw',
    'depends_on_past': False,
    'start_date': dates.days_ago(0)
}

goodtables_dag =  DAG("goodtables", default_args=default_args, max_active_runs=1, catchup=False, schedule_interval=None)
schema_path = os.getcwd() + '/datapackage.json'

def validate_using_goodtables(schema_path):
  inspector = Inspector(row_limit=2000)
  wpdx_report = inspector.inspect(schema_path, preset='datapackage')
  pprint(wpdx_report)


t1 = PythonOperator(
    task_id="validate_using_goodtables",
    python_callable=validate_using_goodtables,
    op_kwargs={"schema_path": schema_path},
    dag=goodtables_dag
)

t1