"""
Airflow Defer-based Async Benchmark DAG

This DAG uses Airflow's defer/trigger mechanism to perform async file I/O operations.
It measures the performance of deferred operations for comparison with asyncio approach.
"""

import time
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.context import Context
from async_file_triggers import AsyncFileWriteTrigger
#from async_file_triggers_lessblocking import AsyncFileWriteTrigger


class DeferFileWriteOperator(BaseOperator):
    """Operator that uses defer mechanism for async file operations."""

    def __init__(self, file_path: str, content: str, delay: float = 0.1, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.content = content
        self.delay = delay

    def execute(self, context: Context):
        """Start the deferred async operation."""
        start_time = time.time()
        context['task_instance'].xcom_push(key='start_time', value=start_time)

        self.log.info(f"Starting deferred file write to {self.file_path}")

        self.defer(
            trigger=AsyncFileWriteTrigger(
                file_path=self.file_path,
                content=self.content,
                delay=self.delay,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """Complete the deferred operation."""
        start_time = context['task_instance'].xcom_pull(key='start_time')
        total_time = time.time() - start_time

        self.log.info(f"Deferred file write completed in {total_time:.4f}s")
        self.log.info(f"Trigger execution time: {event['execution_time']:.4f}s")

        return {
            "status": event["status"],
            "file_path": event["file_path"],
            "trigger_time": event["execution_time"],
            "total_time": total_time,
            "content_length": event["content_length"],
        }


# DAG definition
default_args = {
    'owner': 'benchmark',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id='async_benchmark_defer',
    default_args=default_args,
    description='Async benchmark using Airflow defer mechanism',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['benchmark', 'async', 'defer'],
) as dag:

    # Create multiple file write operations to test concurrency
    write_ops = []
    num_operations = 10
    base_path = "/Users/alexgo/code/airflow/benchmark_output/defer"

    for i in range(num_operations):
        content = f"Defer-based async operation {i+1}\n" + "x" * 1000  # 1KB content
        file_path = f"{base_path}/file_{i+1:03d}.txt"

        op = DeferFileWriteOperator(
            task_id=f'defer_write_{i+1:03d}',
            file_path=file_path,
            content=content,
            delay=0.5,  # Half second delay to simulate I/O
        )
        write_ops.append(op)

    # Create a summary task
    from airflow.providers.standard.operators.python import PythonOperator

    def summarize_results(**context):
        """Collect and summarize benchmark results."""
        results = []
        total_time = 0

        for i in range(num_operations):
            task_id = f'defer_write_{i+1:03d}'
            result = context['task_instance'].xcom_pull(task_ids=task_id)
            if result:
                results.append(result)
                total_time += result['total_time']

        avg_time = total_time / len(results) if results else 0

        summary = {
            'approach': 'defer',
            'num_operations': len(results),
            'total_time': total_time,
            'avg_time_per_op': avg_time,
            'operations_per_second': len(results) / total_time if total_time > 0 else 0,
        }

        print(f"=== DEFER BENCHMARK RESULTS ===")
        print(f"Operations: {summary['num_operations']}")
        print(f"Total time: {summary['total_time']:.4f}s")
        print(f"Avg time per op: {summary['avg_time_per_op']:.4f}s")
        print(f"Operations/sec: {summary['operations_per_second']:.2f}")
        print(f"===============================")

        return summary

    summary_task = PythonOperator(
        task_id='summarize_defer_results',
        python_callable=summarize_results,
    )

    # Set up dependencies - all write operations run in parallel, then summary
    write_ops >> summary_task


# Test the DAG when run directly
if __name__ == "__main__":
    print("Testing Defer benchmark DAG...")
    dag.test()
