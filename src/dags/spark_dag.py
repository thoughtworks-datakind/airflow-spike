# from __future__ import print_function
# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils import dates
# from airflow import DAG
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from datetime import datetime, timedelta
# import os


# DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

# APPLICATION_FILE_PATH = os.getcwd() + "/src/spark_code.py"

# default_args = {
#     'owner': 'datakind',
#     'depends_on_past': False,
#     'retries': 0,
#     }

# _config = {
#         # 'conf': {
#         #     'parquet.compression': 'SNAPPY'
#         # },

#         # 'py_files': 'sample_library.py',
#         'py_files': 'spark_code.py',
#         # 'driver_classpath': 'parquet.jar',
#         # 'jars': 'parquet.jar',
#         # 'packages': 'com.databricks:spark-avro_2.11:3.2.0',
#         # 'exclude_packages': 'org.bad.dependency:1.0.0',
#         # 'repositories': 'http://myrepo.org',
#         'total_executor_cores': 4,
#         'executor_cores': 4,
#         'executor_memory': '22g',
#         # 'keytab': 'privileged_user.keytab',
#         # 'principal': 'user/spark@airflow.org',
#         'name': '{{ task_instance.task_id }}',
#         'num_executors': 10,
#         'verbose': True,
#         # 'application': 'test_application.py',
#         'application': APPLICATION_FILE_PATH,
#         'driver_memory': '3g',
#         'java_class': 'com.foo.bar.AppMain',
#         'application_args': [
#             '-f', 'foo',
#             '--bar', 'bar',
#             '--start', '{{ macros.ds_add(ds, -1)}}',
#             '--end', '{{ ds }}',
#             '--with-spaces', 'args should keep embdedded spaces',
#         ]
#     }
#   # Given




# test_dag = DAG(DAG_ID, default_args=default_args, schedule_interval=None, start_date=(datetime.now() - timedelta(minutes=1)))



# dummy = SparkSubmitOperator(
#     task_id='spark-submit-python',
#     application_file=APPLICATION_FILE_PATH,
#     application_args="arg1 arg2",
#     dag=test_dag)

# def validate(*args, **kwargs):
#     print("validating here")

# t1 = BashOperator(
#     task_id='print_date',
#     bash_command='date',
#     dag=test_dag)

# operator = SparkSubmitOperator(task_id='spark_submit_job',
#     dag=test_dag, **_config)

# t2 = PythonOperator(
#     task_id="validate",
#     python_callable=validate,
#     provide_context=True,
#     dag=test_dag)

# t1 >> operator >> t2

from airflow import DAG
import os
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta


args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 24)
}
dag = DAG('spark_dag', default_args=args, schedule_interval="*/10 * * * *")

operator = SparkSubmitOperator(
    task_id='spark_submit_job',
    application=os.getcwd()+"/src/spark_code.py",
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='1',
    name='airflow-spark',
    verbose=False,
    driver_memory='1g',
    dag=dag,
)

operator