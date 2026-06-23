from airflow.sdk import dag,task,task_group

@dag
def group():
    @task
    def a():
        return 42
    @task_group(default_args = {'retries':2} )
    def my_group(val:int):

        @task
        def b(incomming_val:int):
            print(incomming_val+42)
        @task_group(default_args={'retries':3})
        def nested_group():
            @task 
            def d():
                print('D')
            @task 
            def e():
                print('E')
            d() >> e() 
        @task
        def c():
            print('C')
        b(val) >> nested_group() >> c()
    
    val=a() 
    my_group(val) 
group()