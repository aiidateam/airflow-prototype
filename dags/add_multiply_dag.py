# Airflow 2.x imports:
from airflow.decorators import dag, task, task_group
# If you're on Airflow 3.x, swap the line above for:
# from airflow.sdk import dag, task, task_group

@dag(
    dag_id="add_multiply_dag",
    schedule=None,                    # run on-demand
    catchup=False,
    tags=["example", "math"],
)
def add_multiply_dag():

    @task
    def add(x: int, y: int) -> int:
        return x + y

    @task
    def multiply(x: int, y: int) -> int:
        return x * y

    @task_group
    def AddMultiply(x: int, y: int, z: int):
        the_sum = add(x=x, y=y)            # returns an XComArg
        product = multiply(x=the_sum, y=z) # pass XComArg directly
        return product                      # task groups can return the inner task's XComArg

    # Example inputs; in real DAGs you might pull from params or datasets
    result = AddMultiply(x=2, y=3, z=5)

    # Optional: log/inspect the final result
    @task
    def show(res: int):
        print(f"Final result: {res}")

    show(result)

dag = add_multiply_dag()
