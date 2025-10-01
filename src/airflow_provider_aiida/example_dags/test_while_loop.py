from datetime import datetime
from typing import Any
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow_provider_aiida.taskgroups.utils import WhileTaskGroup


class CounterWhileLoop(WhileTaskGroup):
    """Simple counter while loop that increments until target is reached"""

    def __init__(self, group_id: str, target: int, **kwargs):
        self.target = target
        self.counter = 0
        super().__init__(group_id, max_iterations=100, **kwargs)

    def condition(self, iteration: int, prev_result: Any = None, **context) -> bool:
        """Continue while counter is less than target"""
        # Use previous result if available
        if prev_result:
            counter = prev_result['result']
        else:
            counter = self.counter

        print(f"Checking condition: counter={counter}, target={self.target}")
        return counter < self.target

    def body(self, iteration: int, prev_result: Any = None, **context) -> Any:
        """Increment counter"""
        # Use previous result if available
        if prev_result:
            counter = prev_result['result']
        else:
            counter = self.counter

        counter += 1
        print(f"Iteration {iteration}: counter incremented to {counter}")
        return counter


# Create DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'test_while_loop',
    default_args=default_args,
    description='Test WhileTaskGroup with simple counter',
    schedule=None,
    catchup=False,
    tags=['test', 'while', 'taskgroup'],
    params={
        "target": Param(5, type="integer", description="Target count to reach"),
    }
) as dag:

    # Create while loop task group
    counter_loop = CounterWhileLoop(
        group_id="counter_loop",
        target=5,  # Fixed value for now
    )

    @task
    def report_results():
        """Report the final results from the while loop"""
        from airflow.sdk import get_current_context
        context = get_current_context()
        task_instance = context['task_instance']

        # Get the while loop result
        loop_result = task_instance.xcom_pull(
            task_ids='counter_loop.while_loop',
        )

        print(f"While loop completed: {loop_result}")
        return loop_result

    # Set up task dependencies
    counter_loop >> report_results()
