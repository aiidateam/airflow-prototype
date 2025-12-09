"""Example DAG demonstrating the ConditionalLoopTaskGroup."""

from airflow import DAG
from airflow_provider_aiida.operators.conditional_loop import ConditionalLoopTaskGroup


def check_condition(**context):
    ti = context['ti']
    iteration = ti.xcom_pull(key='iteration_count') or 0
    # Continue if iteration < 3
    should_continue = iteration < 3
    print(f"Should continue: {should_continue}")

    return should_continue

def step_1(**context):
    pass

def step_2(**context):
    pass

def step_3(**context):
    pass


with DAG('conditional_loop_example') as dag:

    # Create the conditional loop task group
    loop = ConditionalLoopTaskGroup(
        group_id='my_loop',
        condition_callable=check_condition,
        step_callables=[step_1, step_2, step_3],
        max_iterations=3,  # Safety limit
    )
