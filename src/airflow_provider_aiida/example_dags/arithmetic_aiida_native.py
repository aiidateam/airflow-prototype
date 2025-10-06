"""
Example DAG using AsyncAiiDACalcJobTaskGroup with Native AiiDA Task Functions

This demonstrates native AiiDA CalcJob execution in Airflow using the task functions
from aiida-core's calcjobs.tasks module through async triggers.

NOTE: This is a simplified example that demonstrates the structure. In a real implementation,
you would need proper AiiDA initialization, database setup, and computer configuration.
"""

from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

from aiida.engine import CalcJobProcessSpec
from aiida.orm import Int, Str

from airflow_provider_aiida.taskgroups.async_aiida_calcjob import SimpleAsyncAiiDACalcJobTaskGroup


class AiiDAAddJobTaskGroup(SimpleAsyncAiiDACalcJobTaskGroup):
    """Addition job using native AiiDA CalcJob task functions."""

    @classmethod
    def define(cls, spec: CalcJobProcessSpec):
        """Define the input specification using AiiDA's spec system."""
        super().define(spec)
        spec.input('x', valid_type=Int, help='First operand')
        spec.input('y', valid_type=Int, help='Second operand')
        spec.input('computer', valid_type=Str, help='Computer label')
        spec.input('code', valid_type=Str, required=False, help='Code label')

    def prepare_for_submission(self, folder):
        """Prepare the calculation for submission.

        Write input files and return CalcInfo with job details.
        """
        from aiida.common.datastructures import CalcInfo, CodeInfo

        # Get input values from self.resolved_inputs (similar to AiiDA's self.inputs)
        x = self.resolved_inputs.x
        y = self.resolved_inputs.y

        # Write a simple bash script that does addition
        with folder.open('add.sh', 'w') as f:
            f.write('#!/bin/bash\n')
            f.write(f'# Addition script: {x} + {y}\n')
            f.write(f'echo $(({x} + {y})) > result.out\n')

        # Make script executable
        import os
        os.chmod(folder.get_abs_path('add.sh'), 0o755)

        # Create CalcInfo
        calc_info = CalcInfo()

        # Check if code input is provided
        if hasattr(self.resolved_inputs, 'code') and self.resolved_inputs.code:
            from aiida.orm import load_code
            code = load_code(self.resolved_inputs.code)

            code_info = CodeInfo()
            code_info.code_uuid = code.uuid
            # Use bash -c to execute the script with proper quoting
            code_info.cmdline_params = ['-c', 'bash add.sh']
            code_info.stdout_name = 'add.stdout'
            code_info.stderr_name = 'add.stderr'
            calc_info.codes_info = [code_info]
        else:
            # No code provided - skip code execution for this simple example
            calc_info.codes_info = []

        # Files to retrieve after job completes
        calc_info.retrieve_list = ['result.out', 'add.stdout', 'add.stderr']

        return calc_info

    def parse(self, retrieved_temporary_folder: str, **context) -> dict[str, Any]:
        """Parse the addition results from retrieved files.

        Args:
            retrieved_temporary_folder: Path to folder containing retrieved files (not used, kept for compatibility)

        Returns:
            dict containing exit_status, results, and node_pk
        """
        from aiida.orm import load_node

        # Get the node_pk from prepare task
        ti = context['task_instance']
        node_pk = ti.xcom_pull(task_ids=f'{self.group_id}.prepare_calcjob')

        # Load the node
        node = load_node(node_pk)

        print(f"DEBUG: Node PK = {node_pk}")
        print(f"DEBUG: Node state = {node.get_state()}")
        print(f"DEBUG: Job ID = {node.get_job_id()}")

        # Parse retrieved files from node.outputs.retrieved
        results = {}
        exit_status = 0

        try:
            # Access retrieved files via AiiDA node outputs
            if hasattr(node.outputs, 'retrieved'):
                retrieved = node.outputs.retrieved
                print(f"DEBUG: Retrieved folder found in node.outputs.retrieved")
                print(f"DEBUG: Files in retrieved: {list(retrieved.base.repository.list_object_names())}")

                # Read result.out from retrieved folder
                if 'result.out' in retrieved.base.repository.list_object_names():
                    with retrieved.base.repository.open('result.out', 'r') as f:
                        result_content = f.read().strip()
                        result_value = int(result_content)
                        results['sum'] = result_value

                    # Get input values from node attributes
                    x = node.base.attributes.get('x')
                    y = node.base.attributes.get('y')
                    print(f"Addition result ({x} + {y}): {result_value}")

                    # Create output node and link it to the CalcJob
                    from aiida.orm import Int
                    from aiida.common.links import LinkType
                    output = Int(result_value)
                    output.base.links.add_incoming(node, LinkType.CREATE, link_label='sum')
                    output.store()
                else:
                    print(f"ERROR: result.out not found in retrieved folder")
                    exit_status = 1
            else:
                print(f"ERROR: No retrieved output found on node")
                exit_status = 1

        except Exception as e:
            print(f"ERROR parsing results: {e}")
            import traceback
            traceback.print_exc()
            exit_status = 2

        return {
            'exit_status': exit_status,
            'results': results,
            'node_pk': node_pk
        }


class AiiDAMultiplyJobTaskGroup(SimpleAsyncAiiDACalcJobTaskGroup):
    """Multiplication job using native AiiDA CalcJob task functions."""

    @classmethod
    def define(cls, spec: CalcJobProcessSpec):
        """Define the input specification using AiiDA's spec system."""
        super().define(spec)
        spec.input('x', valid_type=Int, help='First operand')
        spec.input('y', valid_type=Int, help='Second operand')
        spec.input('computer', valid_type=Str, help='Computer label')
        spec.input('code', valid_type=Str, required=False, help='Code label')

    def prepare_for_submission(self, folder):
        """Prepare the calculation for submission.

        Write input files and return CalcInfo with job details.
        """
        from aiida.common.datastructures import CalcInfo, CodeInfo

        # Get input values from self.resolved_inputs (similar to AiiDA's self.inputs)
        x = self.resolved_inputs.x
        y = self.resolved_inputs.y

        # Write a simple bash script that does multiplication
        with folder.open('multiply.sh', 'w') as f:
            f.write('#!/bin/bash\n')
            f.write(f'# Multiplication script: {x} * {y}\n')
            f.write(f'echo $(({x} * {y})) > multiply_result.out\n')
            f.write(f'echo "Performed {x} * {y} = $(({x} * {y}))" > operation.log\n')

        # Make script executable
        import os
        os.chmod(folder.get_abs_path('multiply.sh'), 0o755)

        # Create CalcInfo
        calc_info = CalcInfo()

        # Check if code input is provided
        if hasattr(self.resolved_inputs, 'code') and self.resolved_inputs.code:
            from aiida.orm import load_code
            code = load_code(self.resolved_inputs.code)

            code_info = CodeInfo()
            code_info.code_uuid = code.uuid
            # Use bash -c to execute the script with proper quoting
            code_info.cmdline_params = ['-c', 'bash multiply.sh']
            code_info.stdout_name = 'multiply.stdout'
            code_info.stderr_name = 'multiply.stderr'
            calc_info.codes_info = [code_info]
        else:
            # No code provided - skip code execution for this simple example
            calc_info.codes_info = []

        # Files to retrieve after job completes
        calc_info.retrieve_list = ['multiply_result.out', 'operation.log', 'multiply.stdout', 'multiply.stderr']

        return calc_info

    def parse(self, retrieved_temporary_folder: str, **context) -> dict[str, Any]:
        """Parse the multiplication results from retrieved files."""
        from aiida.orm import load_node

        # Get the node_pk from prepare task
        ti = context['task_instance']
        node_pk = ti.xcom_pull(task_ids=f'{self.group_id}.prepare_calcjob')

        # Load the node
        node = load_node(node_pk)

        # Parse retrieved files from node.outputs.retrieved
        results = {}
        exit_status = 0

        try:
            # Access retrieved files via AiiDA node outputs
            if hasattr(node.outputs, 'retrieved'):
                retrieved = node.outputs.retrieved
                print(f"DEBUG: Retrieved folder found in node.outputs.retrieved")
                print(f"DEBUG: Files in retrieved: {list(retrieved.base.repository.list_object_names())}")

                # Read multiply_result.out from retrieved folder
                if 'multiply_result.out' in retrieved.base.repository.list_object_names():
                    with retrieved.base.repository.open('multiply_result.out', 'r') as f:
                        result_content = f.read().strip()
                        result_value = int(result_content)
                        results['product'] = result_value

                    # Get input values from node attributes
                    x = node.base.attributes.get('x')
                    y = node.base.attributes.get('y')
                    print(f"Multiplication result ({x} * {y}): {result_value}")

                    # Create output node and link it to the CalcJob
                    from aiida.orm import Int
                    from aiida.common.links import LinkType
                    output = Int(result_value)
                    output.base.links.add_incoming(node, LinkType.CREATE, link_label='product')
                    output.store()
                else:
                    print(f"ERROR: multiply_result.out not found in retrieved folder")
                    exit_status = 1

                # Read operation.log if it exists
                if 'operation.log' in retrieved.base.repository.list_object_names():
                    with retrieved.base.repository.open('operation.log', 'r') as f:
                        operation_log = f.read().strip()
                        results['operation_log'] = operation_log
                        print(f"Operation log: {operation_log}")
            else:
                print(f"ERROR: No retrieved output found on node")
                exit_status = 1

        except Exception as e:
            print(f"ERROR parsing results: {e}")
            import traceback
            traceback.print_exc()
            exit_status = 2

        return {
            'exit_status': exit_status,
            'results': results,
            'node_pk': node_pk
        }


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
    'arithmetic_aiida_native',
    default_args=default_args,
    description='Native AiiDA CalcJob TaskGroup using aiida-core task functions',
    schedule=None,
    catchup=False,
    tags=['aiida', 'arithmetic', 'calcjob', 'native', 'async'],
    params={
        "computer": Param("localhost", type="string", description="AiiDA computer label"),
        "code": Param("bash", type="string", description="AiiDA code label for bash execution"),
        "add_x": Param(8, type="integer", description="First operand for addition"),
        "add_y": Param(4, type="integer", description="Second operand for addition"),
        "multiply_x": Param(6, type="integer", description="First operand for multiplication"),
        "multiply_y": Param(9, type="integer", description="Second operand for multiplication"),
    }
) as dag:

    # Create task groups using native AiiDA task functions
    # These will execute via the async triggers in the Airflow triggerer
    # Pass template strings - they'll be converted to AiiDA types at task execution time
    add_job = AiiDAAddJobTaskGroup(
        group_id="aiida_addition_job",
        x="{{ params.add_x }}",
        y="{{ params.add_y }}",
        computer="{{ params.computer }}",
        code="{{ params.code }}",
        submit_script_filename="add.sh",
    )

    multiply_job = AiiDAMultiplyJobTaskGroup(
        group_id="aiida_multiplication_job",
        x="{{ params.multiply_x }}",
        y="{{ params.multiply_y }}",
        computer="{{ params.computer }}",
        code="{{ params.code }}",
        submit_script_filename="multiply.sh",
    )

    @task
    def combine_results(**context):
        """Combine results from both AiiDA CalcJobs.

        This demonstrates accessing results from AiiDA CalcJobs
        that were executed via the async triggers.
        """
        task_instance = context['task_instance']

        # Pull results from both parse tasks
        add_result = task_instance.xcom_pull(
            task_ids='aiida_addition_job.parse'
        )
        multiply_result = task_instance.xcom_pull(
            task_ids='aiida_multiplication_job.parse'
        )

        # Create combined result
        combined = {
            'addition': {
                'exit_status': add_result['exit_status'],
                'success': add_result['exit_status'] == 0,
                'results': add_result['results'],
                'node_pk': add_result['node_pk']
            },
            'multiplication': {
                'exit_status': multiply_result['exit_status'],
                'success': multiply_result['exit_status'] == 0,
                'results': multiply_result['results'],
                'node_pk': multiply_result['node_pk']
            },
            'overall_success': (
                add_result['exit_status'] == 0 and
                multiply_result['exit_status'] == 0
            )
        }

        print("=" * 60)
        print("COMBINED AIIDA CALCJOB RESULTS")
        print("=" * 60)
        print(f"Addition CalcJob (Node {add_result['node_pk']}):")
        print(f"  Status: {'SUCCESS' if combined['addition']['success'] else 'FAILED'}")
        print(f"  Results: {add_result['results']}")
        print()
        print(f"Multiplication CalcJob (Node {multiply_result['node_pk']}):")
        print(f"  Status: {'SUCCESS' if combined['multiplication']['success'] else 'FAILED'}")
        print(f"  Results: {multiply_result['results']}")
        print()
        print(f"Overall: {'ALL SUCCEEDED' if combined['overall_success'] else 'SOME FAILED'}")
        print("=" * 60)

        if not combined['overall_success']:
            raise ValueError("One or more CalcJobs failed")

        return combined

    # Set up workflow: both jobs run in parallel, then combine results
    combine_task = combine_results()
    [add_job, multiply_job] >> combine_task


if __name__ == "__main__":
    from aiida import load_profile
    load_profile()
    """Execute the DAG for testing/debugging."""
    from datetime import datetime

    print("=" * 60)
    print("Testing arithmetic_aiida_native DAG")
    print("=" * 60)

    # Test the DAG with default parameters
    dag.test(
        run_conf={
            "computer": "localhost",
            "code": "bash",
            "add_x": 8,
            "add_y": 4,
            "multiply_x": 6,
            "multiply_y": 9,
        }
    )

    print("\n" + "=" * 60)
    print("DAG test completed!")
    print("=" * 60)
