#!/bin/bash

PID_DIR=/var/run
PID_FILE=$(dirname "$0")/../airflow-webserver.pid

RETVAL=0

if [ -f "$PID_FILE" ]; then
  echo -n "Stopping Airflow WebServer"
  ps aux | grep "[b]in/airflow webserver -p" | awk '{ print $2 }' | xargs kill -s SIGKILL
  ps aux | grep "gunicorn: master \[airflow-webserver\]" | awk '{ print $2 }' | xargs kill -s SIGKILL
  ps aux | grep "gunicorn: worker \[airflow-webserver\]" | awk '{ print $2 }' | xargs kill -s SIGKILL
  rm -f $PID_FILE
else
  echo -n "Airflow WebServer is not running"
fi

exit $RETVAL