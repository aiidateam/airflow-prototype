"""
AsyncIO-based Async Benchmark DAG

This DAG uses traditional asyncio with async queues to perform file I/O operations.
It measures the performance of asyncio approach for comparison with defer mechanism.
"""

import asyncio
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


async def async_file_write(file_path: str, content: str, delay: float = 0.1):
    """Async function to write file with artificial delay."""
    start_time = time.time()

    # Simulate async I/O operation
    await asyncio.sleep(delay)

    # Write file asynchronously
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        import aiofiles
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(content)
    except ImportError:
        # Fallback to thread pool for file I/O if aiofiles not available
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            await loop.run_in_executor(executor, _sync_write_file, file_path, content)

    end_time = time.time()

    return {
        "status": "success",
        "file_path": file_path,
        "execution_time": end_time - start_time,
        "content_length": len(content),
    }


def _sync_write_file(file_path: str, content: str):
    """Helper function for sync file write."""
    with open(file_path, 'w') as f:
        f.write(content)


async def process_file_queue(queue: asyncio.Queue, results: list):
    """Worker coroutine to process file operations from queue."""
    while True:
        try:
            # Get task from queue with timeout
            task_data = await asyncio.wait_for(queue.get(), timeout=1.0)
            if task_data is None:  # Sentinel value to stop
                break

            file_path, content, delay = task_data
            result = await async_file_write(file_path, content, delay)
            results.append(result)
            queue.task_done()

        except asyncio.TimeoutError:
            break
        except Exception as e:
            print(f"Error processing file operation: {e}")
            queue.task_done()


def run_asyncio_benchmark(**context):
    """Run the asyncio-based benchmark."""
    start_time = time.time()

    # Configuration
    num_operations = 10
    num_workers = 5  # Number of async workers
    base_path = "/Users/alexgo/code/airflow/benchmark_output/asyncio"

    async def main():
        # Create queue and results list
        queue = asyncio.Queue()
        results = []

        # Start worker coroutines
        workers = [
            asyncio.create_task(process_file_queue(queue, results))
            for _ in range(num_workers)
        ]

        # Add tasks to queue
        for i in range(num_operations):
            content = f"AsyncIO-based async operation {i+1}\n" + "x" * 1000  # 1KB content
            file_path = f"{base_path}/file_{i+1:03d}.txt"
            await queue.put((file_path, content, 0.5))  # Half second delay

        # Wait for all tasks to complete
        await queue.join()

        # Stop workers
        for _ in workers:
            await queue.put(None)  # Sentinel values

        # Wait for workers to finish
        await asyncio.gather(*workers, return_exceptions=True)

        return results

    # Run the async operations
    try:
        # Check if we're already in an event loop
        loop = asyncio.get_running_loop()
        # If we are, we need to run in a new thread
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(asyncio.run, main())
            results = future.result()
    except RuntimeError:
        # No event loop running, safe to use asyncio.run
        results = asyncio.run(main())

    end_time = time.time()
    total_time = end_time - start_time

    # Calculate statistics
    avg_time = sum(r['execution_time'] for r in results) / len(results) if results else 0

    summary = {
        'approach': 'asyncio',
        'num_operations': len(results),
        'total_time': total_time,
        'avg_time_per_op': avg_time,
        'operations_per_second': len(results) / total_time if total_time > 0 else 0,
        'num_workers': num_workers,
    }

    print(f"=== ASYNCIO BENCHMARK RESULTS ===")
    print(f"Operations: {summary['num_operations']}")
    print(f"Workers: {summary['num_workers']}")
    print(f"Total time: {summary['total_time']:.4f}s")
    print(f"Avg time per op: {summary['avg_time_per_op']:.4f}s")
    print(f"Operations/sec: {summary['operations_per_second']:.2f}")
    print(f"==================================")

    return summary


def compare_results(**context):
    """Compare results from both approaches."""
    asyncio_result = context['task_instance'].xcom_pull(task_ids='run_asyncio_benchmark')

    print(f"\n=== BENCHMARK COMPARISON ===")
    print(f"AsyncIO Approach:")
    print(f"  Total time: {asyncio_result['total_time']:.4f}s")
    print(f"  Operations/sec: {asyncio_result['operations_per_second']:.2f}")
    print(f"  Avg time per op: {asyncio_result['avg_time_per_op']:.4f}s")
    print(f"===========================")

    return {
        'asyncio_results': asyncio_result,
        'comparison_notes': 'Run both DAGs to see full comparison'
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
    dag_id='async_benchmark_asyncio',
    default_args=default_args,
    description='Async benchmark using AsyncIO and queues',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['benchmark', 'async', 'asyncio'],
) as dag:

    # Single task that runs all async operations using asyncio
    asyncio_task = PythonOperator(
        task_id='run_asyncio_benchmark',
        python_callable=run_asyncio_benchmark,
    )

    # Summary and comparison task
    compare_task = PythonOperator(
        task_id='compare_results',
        python_callable=compare_results,
    )

    asyncio_task >> compare_task


# Test the DAG when run directly
if __name__ == "__main__":
    print("Testing AsyncIO benchmark DAG...")
    dag.test()
