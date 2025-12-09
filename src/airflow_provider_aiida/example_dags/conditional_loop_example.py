"""Example DAG demonstrating the ConditionalLoopTaskGroup."""

from airflow import DAG
from airflow_provider_aiida.operators.conditional_loop import ConditionalLoopTaskGroup


def check_condition(**context):
    """Check if we should continue looping.

    This example continues for 3 iterations.
    """
    ti = context['ti']
    iteration = ti.xcom_pull(key='iteration_count') or 0

    print(f"Checking condition at iteration {iteration}")

    # Continue if iteration < 3
    should_continue = iteration < 3
    print(f"Should continue: {should_continue}")

    return should_continue


def step_1(**context):
    """First step to execute."""
    ti = context['ti']
    iteration = ti.xcom_pull(key='iteration_count') or 0

    print(f"Step 1 executing at iteration {iteration}")

    # Store some data in XCom that persists across iterations
    step_1_data = ti.xcom_pull(key='step_1_data') or []
    step_1_data.append(f"step_1_iteration_{iteration}")
    ti.xcom_push(key='step_1_data', value=step_1_data)

    return f"Step 1 completed at iteration {iteration}"


def step_2(**context):
    """Second step to execute."""
    ti = context['ti']
    iteration = ti.xcom_pull(key='iteration_count') or 0

    print(f"Step 2 executing at iteration {iteration}")

    # Access data from step 1
    step_1_data = ti.xcom_pull(key='step_1_data') or []
    print(f"Step 1 has run {len(step_1_data)} times: {step_1_data}")

    # Store some data in XCom
    step_2_data = ti.xcom_pull(key='step_2_data') or []
    step_2_data.append(f"step_2_iteration_{iteration}")
    ti.xcom_push(key='step_2_data', value=step_2_data)

    return f"Step 2 completed at iteration {iteration}"


def step_3(**context):
    """Third step to execute."""
    ti = context['ti']
    iteration = ti.xcom_pull(key='iteration_count') or 0

    print(f"Step 3 executing at iteration {iteration}")

    # Access data from previous steps
    step_1_data = ti.xcom_pull(key='step_1_data') or []
    step_2_data = ti.xcom_pull(key='step_2_data') or []

    print(f"Step 1 data: {step_1_data}")
    print(f"Step 2 data: {step_2_data}")

    return f"Step 3 completed at iteration {iteration}"


with DAG(
    'conditional_loop_example',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    description='Example of conditional loop with steps',
    schedule=None,
    catchup=False,
    tags=['example', 'loop'],
) as dag:

    # Create the conditional loop task group
    loop = ConditionalLoopTaskGroup(
        group_id='my_loop',
        condition_callable=check_condition,
        step_callables=[step_1, step_2, step_3],
        max_iterations=10,  # Safety limit
    )

if __name__ == "__main__":
    # TODO test runs seem not to work this way, why?
    from airflow_provider_aiida.aiida_core import load_profile
    load_profile()
    dag.test()    
