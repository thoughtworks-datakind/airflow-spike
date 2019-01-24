from airflow import DAG
from goodtables import Inspector
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
import os
import json
import pandas as pd
from pprint import pprint

default_args = {
    'owner': 'dk_tw',
    'depends_on_past': False,
    'start_date': dates.days_ago(0)
}

test_dag =  DAG("spike", default_args=default_args, max_active_runs=1, catchup=False, schedule_interval=None)
path = os.getcwd() + '/Water_Point_Data_Exchange_Complete_Dataset.csv'
config_path = os.getcwd() + '/config.json'
schema_path = os.getcwd() + '/datapackage.json'

def validate_config(data_columns, config_path):
    with open(config_path) as file :
        data = json.load(file)
        columns = traverse_json(data["columns"], "name")
        are_columns_valid = check_difference_between_two_lists(columns, data_columns, "Columns not present in data")

        rules = traverse_json(data["columns"], "rules")
        flatmap_rules = [item for sublist in rules for item in sublist]
        are_rules_valid = check_difference_between_two_lists(flatmap_rules, globals().keys(),"Following rules are not defined")

    return are_columns_valid and are_rules_valid

def check_difference_between_two_lists(subset, superset, message):
    diff = [x for x in subset if x not in superset]
    diff_is_empty = len(diff) == 0
    if not diff_is_empty:
        print(message)
        print(diff)
    return diff_is_empty

def traverse_json(input, key):
    list_columns = []
    for entry in input:
        list_columns.append(entry[key])
    return list_columns

def validate_data(input_path, config_path):
    validation_result = 0
    data = pd.read_csv(input_path, low_memory=False)
    if validate_config(data.columns, config_path):
        with open(config_path) as file:
            config = json.load(file)
            for col in config['columns']:
                col_name = col['name']
                rules = col['rules']
                for rule in rules:
                    print('Validating column ' + col_name)
                    result = data[col_name].apply(lambda x:globals()[rule](x)).sum()
                    print('Result is %d' % result)
                    validation_result = validation_result + result
    else:
        print('Invalid configuration')

    return validation_result

def validate_mandatory(input):
    return input == None

def validate_numeric(input):
    return False

def validate_using_goodtables(input_path, schema_path):
  inspector = Inspector()
  report1 = inspector.inspect(schema_path, preset='datapackage', )
  pprint(report1)


t1 = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    op_kwargs={"input_path": path, "config_path": config_path},
    dag=test_dag
)

t2 = PythonOperator(
    task_id="validate_using_goodtables",
    python_callable=validate_using_goodtables,
    op_kwargs={"input_path": path, "schema_path": schema_path},
    dag=test_dag
)

t1
t2
# t1 = BashOperator(
#     task_id='print_date',
#     bash_command='date',
#     dag=test_dag)

# t2 = PythonOperator(
#     task_id="validate",
#     python_callable=validate,
#     provide_context=True,
#     dag=test_dag)