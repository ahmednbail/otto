from  airflow.sdk import dag , task , Asset, Context

@dag()
def xcom_dag():


    @task
    def t1(context: Context):
        val=42
        context['ti'].xcom_push(key='my_key',value=val)


    @task
    def t2(context:Context):
        res=context['ti'].xcom.pull(task_ids='t1' , key='my_key')
        print(res)

    t1() >> t2() 
xcom_dag()