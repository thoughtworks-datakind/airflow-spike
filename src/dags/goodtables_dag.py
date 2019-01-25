from airflow import DAG
from goodtables import validate
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
from pprint import pprint
import os
import json

default_args = {
    'owner': 'dk_tw',
    'depends_on_past': False,
    'start_date': dates.days_ago(0)
}

goodtables_dag =  DAG("goodtables", default_args=default_args, max_active_runs=1, catchup=False, schedule_interval=None)
schema_path = os.getcwd() + '/schema.json'
file_source_path = os.getcwd() + '/Water_Point_Data_Exchange_Complete_Dataset.csv'

def validate_data(file_source_path, schema_path):
  json_file = open(schema_path)
  schema = json.load(json_file)
  json_file.close()
  report = validate(file_source_path, schema=schema, row_limit=2000)
  return report

validation_stage = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    op_kwargs={"file_source_path":file_source_path, "schema_path": schema_path},
    dag=goodtables_dag
)

validation_stage