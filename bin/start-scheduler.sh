#!/bin/bash
set -e

header="Data-Ingestion"
project_path=$(dirname "$0")/../

cd $project_path

export PYTHONPATH=$project_path
export AIRFLOW_HOME=$project_path
echo "$header: running airflow scheduler"
source $project_path/.venv/bin/activate
nohup airflow scheduler >> $project_path/logs/airflow_scheduler.log 2>&1 &

