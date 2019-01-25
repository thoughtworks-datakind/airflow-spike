from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
import os
import pandas as pd
import json
from commons.common_functions import validate_config, validate_mandatory, validate_numeric

default_args = {
    'owner': 'dk_tw',
    'depends_on_past': False,
    'start_date': dates.days_ago(0)
}

custom_checker_dag =  DAG("custom_checker", default_args=default_args, max_active_runs=1, catchup=False, schedule_interval=None)
path = os.getcwd() + '/Water_Point_Data_Exchange_Complete_Dataset.csv'
config_path = os.getcwd() + '/config.json'


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


t1 = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    op_kwargs={"input_path": path, "config_path": config_path},
    dag=custom_checker_dag
)

t1