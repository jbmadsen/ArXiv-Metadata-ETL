# Copy config file to be reachable from docker container
docker cp ./aws.cfg airflow_webserver:/usr/local/aws.cfg;
docker cp ./add_airflow_connections.py airflow_webserver:/usr/local/add_airflow_connections.py;

# Adding /entrypoint.sh before bash:
# https://github.com/puckel/docker-airflow/issues/505
# https://github.com/apache/airflow/pull/3684

# Setting up all Connections in Airflow
docker exec -it airflow_webserver /entrypoint.sh bash -c 'cd ..; python ./add_airflow_connections.py;';
