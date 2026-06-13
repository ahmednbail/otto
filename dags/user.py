# pyrefly: ignore [missing-import]
from airflow.sdk import asset , Asset , Context
import requests
from typing import Any 

@asset(
    schedule = "@daily",
    uri = 'https://randomuser.me/api/')

def user(self) ->dict[str]: #---> type hinting for the return value or callled typing
    r = requests.get(self.uri)
    return r.json()


@asset.multi(
    schedule = user,
    outlets = [
        Asset(name='user_location') ,
        Asset(name='user_login') 
        ]
)
def user_info(user : Asset , context :Context)->list[dict[str]]:
    user_data=context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids=user.name,
        key='return_value',
        include_prior_dates = True ) # type: ignore[unexpected-keyword]
    
    return [
        user_data[0]['results'][0]['location'],
        user_data[0]['results'][0]['login']
        ]


'''
@asset(schedule = user)
def user_location(user : Asset ,context : Context ) -> dict[str, Any]:
    user_data = context['ti'].xcom_pull(
        dag_id = user.name ,
        task_ids = user.name,
        key='return_value',
        include_prior_dates = True ) # type: ignore[unexpected-keyword]
    
    return user_data[0]['results'][0]['location']


@asset(schedule = user)
def user_login(user :Asset, context :Context) -> dict[str,Any]:
    user_data=context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids = user.name,
        key='return_value',
        include_prior_dates = True )
    return user_data[0]['results'][0]['login']

'''