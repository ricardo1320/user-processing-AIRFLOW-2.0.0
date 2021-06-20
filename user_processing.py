#Imports
from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
import json
from pandas import json_normalize

#Dictionary with the common arguments for all the tasks
default_args = {
    'start_date': datetime(2021,5,1)
}

#Process the JSON object and filter just the fields that we want from the user to store in our SQLite table.
def _processing_user(ti):
    #ti = Task Instance
    #xcom's is a mechanism to share data between different tasks.
    #Here, with 'xcom_pull' we get the user-Json-object from the last Task which has id=extracting_user
    users = ti.xcom_pull(task_ids=['extracting_user'])

    #Raise an error if the variable is empty or is not what we expected
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    
    #Put the a single user in a variable
    user = users[0]['results'][0]

    #Dictionary for the processed user
    processed_user = {
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    }

    #Pandas dataframe with the user information
    processed_user = json_normalize(processed_user)

    #Convert pandas dataframe to a CSV file
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)



#Create a DAG object
with DAG('user_processing', schedule_interval='@daily', default_args=default_args, catchup=True) as dag:
    #Define tasks/operators
    #Task of type TRANSFER. This task will execute a Sqlite command. Create users table.
    creating_table = SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='db_sqlite',
        sql='''
            CREATE TABLE IF NOT EXISTS users(
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT PRIMARY KEY
            );
            '''
    )

    #Task of type SENSOR. This task will wait for the API to be available before moving to the next task.
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    #Task of type ACTION. This task will get an user from the API in JSON format.
    extracting_user = SimpleHttpOperator(
        task_id='extracting_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    #Task of type ACTION. This task will execute a Python function.
    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=_processing_user
    )

    #Task of type ACTION. This task will execute a bash command. Insert the csv-user into the 'users' Sqlite table.
    storing_user = BashOperator(
        task_id='storing_user',
        bash_command='echo -e ".separator ","\n.import /tmp/processed_user.csv users" | sqlite3 /home/airflow/airflow/airflow.db'
    )

    #Define the dependencies between the tasks:
    creating_table >> is_api_available >> extracting_user >> processing_user >> storing_user



#Airflow commands:
#   airflow webserver -> start the webserver
#   airflow scheduler -> start the scheduler
#   airflow tasks test user_processing creating_table 2020-01-01 -> test each task by its own