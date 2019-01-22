##Steps to run the spike

From the project directory,

1. Run setup script - ./bin/setup.sh
2. 2 files will get created - airflow.cfg (config file for airflow), airflow.db (the metadata database)
3. For now, need to set the properties in airflow.cfg manually - set airflow_home and dags_folder to the root directory and the dag directory path respectively.
4. Start the web server - ./bin/start-server.sh
5. Start the scheduler - ./bin/start-scheduler.sh
Airflow web instance should be running at localhost:8080