#!/bin/bash
set -e

port_number=8080
project_path=$(dirname "$0")/../

cd $project_path

export PYTHONPATH=$project_path
export AIRFLOW_HOME=$project_path

echo "Starting Airflow WebServer on port: $port_number"
source $project_path/.venv/bin/activate
mkdir -p $project_path/logs
nohup airflow webserver -p $port_number 2>&1 >> $project_path/logs/airflow.log &
