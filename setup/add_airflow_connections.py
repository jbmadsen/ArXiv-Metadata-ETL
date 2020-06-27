# Imports
import configparser
import json
from ast import literal_eval
from airflow.settings import Session
from airflow.models import Connection


def create_connection(config):
    """Creates a Connection object for Airflow

    Args:
        config (Dictionary): A dictionary containing all information needed to create a Connection object
    """
    print(f"Creating and adding connection {config['conn_id']}")

    # Create connection object
    conn = Connection(
            conn_id = config['conn_id'],
            conn_type = config['conn_type'],
            #host = config['host'],
            #schema = config['schema'],
            #login = config['login'],
            #password = config['password'],
            #port = config['port'],
            #extra = json.dumps(config['extra'])
    ) 

    if config['login'] is not None:
        conn.login = config['login']

    if config['password'] is not None:
        conn.password = config['password']

    if config['host'] is not None:
        conn.host = config['host']

    if config['schema'] is not None:
        conn.schema = config['schema']

    if config['port'] is not None:
        conn.port = config['port']
    
    if config['extra'] is not None:
        conn.extra = json.dumps(config['extra'])
    
    connection = Session.query(Connection).filter_by(conn_id=conn.conn_id).first()
    if connection is None:
        # Add connection object to session, and commit
        Session.add(conn)
        Session.commit() 
        print(f"Connection {config['conn_id']} added and committed")
    else:
        print(f"Connection {config['conn_id']} already exists")


if __name__ == "__main__":
    # Starting
    print("Setting up connections")

    # Load configurations
    config = configparser.ConfigParser()
    config.read_file(open('./aws.cfg'))

    # List of connections to create
    connections = []
    
    connections.append(
        {
            'conn_id': 'aws_credentials',
            'conn_type': 'aws',
            'host': None,
            'schema': None,
            'login': config['AWS']['KEY'],
            'password': config['AWS']['SECRET'],
            'port': None,
            'extra': None,
        }
    )

    connections.append(
        {
            'conn_id': 's3_credentials',
            'conn_type': 's3',
            'host': None,
            'schema': None,
            'login': None,
            'password': None,
            'port': None,
            'extra': { "aws_access_key_id": config['AWS']['KEY'], "aws_secret_access_key": config['AWS']['SECRET'] }
        }
    )

    connections.append(
        {
            'conn_id': 'redshift',
            'conn_type': 'postgres',
            'host': config['CLUSTER']['DB_HOST'],
            'schema': config['CLUSTER']['DB_NAME'],
            'login': config['CLUSTER']['DB_USER'],
            'password': config['CLUSTER']['DB_PASSWORD'],
            'port': config['CLUSTER']['DB_PORT'],
            'extra': None,
        }
    )

    # Add all configurations
    for conn in connections:
        create_connection(conn)

    # Done
    print("Setting up connections complete")
