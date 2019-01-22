#!/bin/bash

RETVAL=0

ps aux | grep -q "[b]in/airflow scheduler"
result=$?

if [ $result -eq 0 ]; then
  echo -n "Stopping Airflow Scheduler"
  ps aux | grep "[b]in/airflow scheduler" | awk '{ print $2 }' | xargs kill -s SIGKILL
else
  echo -n "Airflow Scheduler is not running"
fi

exit $RETVAL