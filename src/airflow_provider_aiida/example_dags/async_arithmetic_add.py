from pathlib import Path
from datetime import datetime
from typing import Dict, Any
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow_provider_aiida.taskgroups.async_calcjob import AsyncCalcJobTaskGroup


class AsyncAddJobTaskGroup(AsyncCalcJobTaskGroup):
    """Async Addition job task group - uses deferrable operators"""

    def __init__(self, group_id: str, machine: str, local_workdir: str, remote_workdir: str,
                 x: int, y: int, sleep: int, **kwargs):
        self.x = x
        self.y = y
        self.sleep = sleep
        super().__init__(group_id, machine, local_workdir, remote_workdir, **kwargs)

    def prepare(self, **context) -> Dict[str, Any]:
        """Prepare addition job inputs"""
        to_upload_files = {}

        # Get rendered values from params
        params = context.get('params', {})
        x = params.get('add_x', self.x)
        y = params.get('add_y', self.y)

        submission_script = f"""
sleep {self.sleep}
echo "$(({x}+{y}))" > result.out
        """

        to_receive_files = {"result.out": "addition_result.txt"}

        # Push to XCom for the async calcjob operators to use
        context['task_instance'].xcom_push(key='to_upload_files', value=to_upload_files)
        context['task_instance'].xcom_push(key='submission_script', value=submission_script)
        context['task_instance'].xcom_push(key='to_receive_files', value=to_receive_files)

        return {
            "to_upload_files": to_upload_files,
            "submission_script": submission_script,
            "to_receive_files": to_receive_files
        }

    def parse(self, local_workdir: str, **context) -> tuple[int, Dict[str, Any]]:
        """Parse addition job results"""
        to_receive_files = context['task_instance'].xcom_pull(
            task_ids=f'{self.group_id}.prepare',
            key='to_receive_files'
        )

        results = {}
        exit_status = 0  # Start with success

        try:
            for file_key, received_file in to_receive_files.items():
                file_path = Path(local_workdir) / Path(received_file)
                if not file_path.exists():
                    print(f"ERROR: Expected file {received_file} not found")
                    exit_status = 1
                    continue

                result_content = file_path.read_text().strip()
                print(f"Addition result ({self.x} + {self.y}): {result_content}")
                results[file_key] = int(result_content)

        except Exception as e:
            print(f"ERROR parsing results: {e}")
            exit_status = 2

        # Store both exit status and results in XCom
        final_result = (exit_status, results)
        context['task_instance'].xcom_push(key='final_result', value=final_result)
        return final_result


class AsyncMultiplyJobTaskGroup(AsyncCalcJobTaskGroup):
    """Async Multiplication job task group - uses deferrable operators"""

    def __init__(self, group_id: str, machine: str, local_workdir: str, remote_workdir: str,
                 x: int, y: int, sleep: int, **kwargs):
        self.x = x
        self.y = y
        self.sleep = sleep
        super().__init__(group_id, machine, local_workdir, remote_workdir, **kwargs)

    def prepare(self, **context) -> Dict[str, Any]:
        """Prepare multiplication job inputs"""
        to_upload_files = {}

        # Get rendered values from params
        params = context.get('params', {})
        x = params.get('multiply_x', self.x)
        y = params.get('multiply_y', self.y)

        submission_script = f"""
sleep {self.sleep}
echo "$(({x}*{y}))" > multiply_result.out
echo "Operation: {x} * {y}" > operation.log
        """

        to_receive_files = {
            "multiply_result.out": "multiply_result.txt",
            "operation.log": "operation.log"
        }

        # Push to XCom
        context['task_instance'].xcom_push(key='to_upload_files', value=to_upload_files)
        context['task_instance'].xcom_push(key='submission_script', value=submission_script)
        context['task_instance'].xcom_push(key='to_receive_files', value=to_receive_files)

        return {
            "to_upload_files": to_upload_files,
            "submission_script": submission_script,
            "to_receive_files": to_receive_files
        }

    def parse(self, local_workdir: str, **context) -> tuple[int, Dict[str, Any]]:
        """Parse multiplication job results"""
        to_receive_files = context['task_instance'].xcom_pull(
            task_ids=f'{self.group_id}.prepare',
            key='to_receive_files'
        )

        results = {}
        exit_status = 0  # Start with success

        try:
            for file_key, received_file in to_receive_files.items():
                file_path = Path(local_workdir) / Path(received_file)
                if not file_path.exists():
                    print(f"ERROR: Expected file {received_file} not found")
                    exit_status = 1
                    continue

                content = file_path.read_text().strip()
                print(f"File {file_key}: {content}")
                if file_key == "multiply_result.out":
                    results['result'] = int(content)
                else:
                    results['log'] = content

            if 'result' not in results:
                print("ERROR: No multiplication result found")
                exit_status = 1

            print(f"Multiplication result ({self.x} * {self.y}): {results.get('result', 'N/A')}")

        except Exception as e:
            print(f"ERROR parsing results: {e}")
            exit_status = 2

        # Store both exit status and results in XCom
        final_result = (exit_status, results)
        context['task_instance'].xcom_push(key='final_result', value=final_result)
        return final_result


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
    'async_arithmetic_add_multiply',
    default_args=default_args,
    description='Async CalcJob TaskGroup for ArithmeticAddMultiply using deferrable operators',
    schedule=None,
    catchup=False,
    tags=['arithmetics', 'calcjob', 'async', 'taskgroup'],
    params={
        "machine": Param("localhost", type="string"),
        "local_workdir": Param("/tmp/airflow/local_workdir", type="string"),
        "remote_workdir": Param("/tmp/airflow/remote_workdir", type="string"),
        "add_x": Param(10, type="integer", description="First operand for addition"),
        "add_y": Param(5, type="integer", description="Second operand for addition"),
        "multiply_x": Param(7, type="integer", description="First operand for multiplication"),
        "multiply_y": Param(8, type="integer", description="Second operand for multiplication"),
    }
) as dag:

    # Create async task groups - these will use deferrable operators
    async_add_job = AsyncAddJobTaskGroup(
        group_id="async_addition_job",
        machine="{{ params.machine }}",
        local_workdir="{{ params.local_workdir }}/async_addition_job",
        remote_workdir="{{ params.remote_workdir }}/async_addition_job",
        x="{{ params.add_x }}",
        y="{{ params.add_y }}",
        sleep=2,
    )

    async_multiply_job = AsyncMultiplyJobTaskGroup(
        group_id="async_multiplication_job",
        machine="{{ params.machine }}",
        local_workdir="{{ params.local_workdir }}/async_multiplication_job",
        remote_workdir="{{ params.remote_workdir }}/async_multiplication_job",
        x="{{ params.multiply_x }}",
        y="{{ params.multiply_y }}",
        sleep=1,
    )

    @task
    def combine_results():
        """Combine results from both async job types"""
        from airflow.sdk import get_current_context
        context = get_current_context()
        task_instance = context['task_instance']

        add_result = task_instance.xcom_pull(
            task_ids='async_addition_job.parse',
            key='final_result'
        )
        multiply_result = task_instance.xcom_pull(
            task_ids='async_multiplication_job.parse',
            key='final_result'
        )

        # Unpack tuples (exit_status, results)
        add_exit_status, add_data = add_result
        multiply_exit_status, multiply_data = multiply_result

        combined = {
            'addition': {
                'exit_status': add_exit_status,
                'success': add_exit_status == 0,
                'data': add_data
            },
            'multiplication': {
                'exit_status': multiply_exit_status,
                'success': multiply_exit_status == 0,
                'data': multiply_data
            },
            'overall_success': add_exit_status == 0 and multiply_exit_status == 0
        }

        print(f"Combined async results: {combined}")
        return combined

    # Direct usage - async task groups ARE TaskGroups!
    combine_task = combine_results()
    [async_add_job, async_multiply_job] >> combine_task
