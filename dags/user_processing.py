# pyrefly: ignore [missing-import]
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator #-----> SQL execution operator for postgres
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import asset
from datetime import datetime 
import requests

'''
def _extract_user(ti):  #-------- ti is reserved word for airflow for passing data between tasks
    #fake_user = ti.xcom_pull(task_ids = 'is_api_available')
    response = requests.get('https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json')
    fake_user = response.json()
    return {
        'id' : fake_user['id'],
        'firstname' : fake_user['personalInfo']['firstName'],
        'lastname' : fake_user['personalInfo']['lastName'],
        'email' : fake_user['personalInfo']['email'],
    }
''' #-------> this is python operator but we are going to use task decorator for it


@dag(dag_id="user_processing",schedule=None,start_date=datetime(2026,6,8))
def user_processing():
    create_table=SQLExecuteQueryOperator(
        task_id = 'create_table',
        conn_id='postgres',
        sql= """ 
                 CREATE TABLE IF NOT EXISTS users( 
                        id INT PRIMARY KEY, 
                        firstname VARCHAR(255),
                        lastname VARCHAR(255), 
                        email VARCHAR(255), 
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                )
            """
    )

    @task.sensor(poke_interval=30,timeout=300)
    def is_api_available() -> PokeReturnValue:

        response = requests.get('https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json')
        print(response.status_code)
        if response.status_code == 200:
              condition = True
              fake_user = response.json()
        else: 
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition,xcom_value=fake_user)


    @task
    def _extract_user(fake_user):
        return {
            'id' : fake_user['id'],
            'firstname' : fake_user['personalInfo']['firstName'],
            'lastname' : fake_user['personalInfo']['lastName'],
            'email' : fake_user['personalInfo']['email'],
        }
    
        ''' #----------> This is python operator but we are going to use task decorator for it 
        extract_user = PythonOperator(
            task_id = "extract_user",
            python_callable  = _extract_user  
        )
        ''' 
    @task
    def process_user(user_info):
       import csv
       user_info['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       with open('/tmp/user_info.csv','w',newline='') as f:
         
           writer = csv.DictWriter(f,fieldnames=user_info.keys())
           writer.writeheader()
           writer.writerow(user_info)
    
    @task 
    def store_user():
        hook = PostgresHook(postgres_conn_id='postgres')
        hook.get_conn()
        hook.copy_expert(
            sql='COPY users FROM STDIN WITH CSV HEADER',
            filename='/tmp/user_info.csv'
        )

    process_user(_extract_user(create_table >>is_api_available())) >> store_user()

user_processing()


 