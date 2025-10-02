"""
Example DAG using AiiDA-style ArithmeticAddCalculation CalcJob.

This demonstrates how to use the CalcJob TaskGroup interface in an Airflow DAG.
It mimics the interface of aiida.calculations.arithmetic.add.ArithmeticAddCalculation.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional
import io

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

from airflow_provider_aiida.taskgroups.aiida_calcjob import (
    CalcJob,
    CalcInfo,
    CodeInfo,
    ExitCode,
    Folder,
)


class ArithmeticAddCalculation(CalcJob):
    """
    CalcJob to add two numbers using bash.

    This CalcJob:
    1. Creates a bash script that adds two numbers
    2. Executes the script
    3. Parses the output to extract the sum
    4. Validates the result
    """

    # Custom exit codes
    ERROR_READING_OUTPUT_FILE = ExitCode(
        310,
        "The output file could not be read.",
        invalidates_cache=True
    )
    ERROR_INVALID_OUTPUT = ExitCode(
        320,
        "The output file contains invalid output.",
        invalidates_cache=True
    )
    ERROR_NEGATIVE_NUMBER = ExitCode(
        410,
        "The sum of the operands is a negative number."
    )

    def __init__(
        self,
        group_id: str,
        computer: str,
        x: int,
        y: int,
        sleep: int = 0,
        input_filename: str = 'aiida.in',
        output_filename: str = 'aiida.out',
        **kwargs
    ):
        super().__init__(
            group_id=group_id,
            computer=computer,
            metadata={
                'options': {
                    'input_filename': input_filename,
                    'output_filename': output_filename,
                    'resources': {'num_machines': 1, 'num_mpiprocs_per_machine': 1},
                }
            },
            **kwargs
        )

        self.x = x
        self.y = y
        self.sleep = sleep
        self.input_filename = input_filename
        self.output_filename = output_filename

    def prepare_for_submission(self, folder: Folder) -> CalcInfo:
        """
        Prepare the calculation for submission.

        Creates a bash script that:
        1. Optionally sleeps for specified time
        2. Echoes the sum of x and y
        """
        script_content = ""

        if self.sleep > 0:
            script_content += f"sleep {self.sleep}\n"

        script_content += f"echo $(({self.x} + {self.y}))\n"

        # Write input script
        folder.create_file_from_filelike(
            io.StringIO(script_content),
            self.input_filename,
            mode='w',
            encoding='utf8'
        )

        # Create CodeInfo
        codeinfo = CodeInfo()
        codeinfo.stdin_name = self.input_filename
        codeinfo.stdout_name = self.output_filename
        codeinfo.cmdline_params = ['bash']

        # Create CalcInfo
        calcinfo = CalcInfo()
        calcinfo.codes_info = [codeinfo]
        calcinfo.local_copy_list = [
            ('', folder.get_abs_path(self.input_filename), self.input_filename)
        ]
        calcinfo.retrieve_list = [self.output_filename]

        return calcinfo

    def parse(self, retrieved_temporary_folder: str) -> Optional[ExitCode]:
        """
        Parse the calculation output.

        Reads the output file and extracts the sum.
        """
        output_file = Path(retrieved_temporary_folder) / self.output_filename

        if not output_file.exists():
            print(f"ERROR: Output file {self.output_filename} not found")
            return self.ERROR_READING_OUTPUT_FILE

        try:
            output_content = output_file.read_text().strip()
            print(f"Raw output: {output_content}")
        except Exception as e:
            print(f"ERROR reading output file: {e}")
            return self.ERROR_READING_OUTPUT_FILE

        try:
            result = int(output_content)
            print(f"Parsed result: {self.x} + {self.y} = {result}")
        except ValueError:
            print(f"ERROR: Invalid output, expected integer but got: {output_content}")
            return self.ERROR_INVALID_OUTPUT

        if result < 0:
            print(f"WARNING: Result is negative: {result}")
            return self.ERROR_NEGATIVE_NUMBER

        # Store output
        self.outputs['sum'] = result

        print(f"SUCCESS: {self.x} + {self.y} = {result}")
        return ExitCode(0, "Calculation completed successfully")


class ArithmeticMultiplyCalculation(CalcJob):
    """
    CalcJob to multiply two numbers using bash.

    Similar to ArithmeticAddCalculation but performs multiplication.
    """

    ERROR_READING_OUTPUT_FILE = ExitCode(310, "The output file could not be read.", invalidates_cache=True)
    ERROR_INVALID_OUTPUT = ExitCode(320, "The output file contains invalid output.", invalidates_cache=True)

    def __init__(
        self,
        group_id: str,
        computer: str,
        x: int,
        y: int,
        sleep: int = 0,
        input_filename: str = 'multiply.in',
        output_filename: str = 'multiply.out',
        **kwargs
    ):
        super().__init__(
            group_id=group_id,
            computer=computer,
            metadata={
                'options': {
                    'input_filename': input_filename,
                    'output_filename': output_filename,
                }
            },
            **kwargs
        )

        self.x = x
        self.y = y
        self.sleep = sleep
        self.input_filename = input_filename
        self.output_filename = output_filename

    def prepare_for_submission(self, folder: Folder) -> CalcInfo:
        """Prepare multiplication calculation."""
        script_content = ""

        if self.sleep > 0:
            script_content += f"sleep {self.sleep}\n"

        script_content += f"echo $(({self.x} * {self.y}))\n"

        folder.create_file_from_filelike(
            io.StringIO(script_content),
            self.input_filename,
            mode='w',
            encoding='utf8'
        )

        codeinfo = CodeInfo()
        codeinfo.stdin_name = self.input_filename
        codeinfo.stdout_name = self.output_filename
        codeinfo.cmdline_params = ['bash']

        calcinfo = CalcInfo()
        calcinfo.codes_info = [codeinfo]
        calcinfo.local_copy_list = [
            ('', folder.get_abs_path(self.input_filename), self.input_filename)
        ]
        calcinfo.retrieve_list = [self.output_filename]

        return calcinfo

    def parse(self, retrieved_temporary_folder: str) -> Optional[ExitCode]:
        """Parse multiplication results."""
        output_file = Path(retrieved_temporary_folder) / self.output_filename

        if not output_file.exists():
            print(f"ERROR: Output file {self.output_filename} not found")
            return self.ERROR_READING_OUTPUT_FILE

        try:
            output_content = output_file.read_text().strip()
            result = int(output_content)
            print(f"Multiplication result: {self.x} * {self.y} = {result}")

            self.outputs['product'] = result

            return ExitCode(0, "Multiplication completed successfully")

        except ValueError:
            print(f"ERROR: Invalid output")
            return self.ERROR_INVALID_OUTPUT
        except Exception as e:
            print(f"ERROR reading output: {e}")
            return self.ERROR_READING_OUTPUT_FILE


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
    'aiida_example_arithmetic_calcjob',
    default_args=default_args,
    description='Example DAG using AiiDA-style CalcJob TaskGroups for arithmetic operations',
    schedule=None,
    catchup=False,
    tags=['example', 'aiida', 'calcjob', 'arithmetic'],
    params={
        "computer": Param(
            "localhost",
            type="string",
            description="Computer to run calculations on"
        ),
        "add_x": Param(
            10,
            type="integer",
            description="First operand for addition"
        ),
        "add_y": Param(
            5,
            type="integer",
            description="Second operand for addition"
        ),
        "multiply_x": Param(
            7,
            type="integer",
            description="First operand for multiplication"
        ),
        "multiply_y": Param(
            8,
            type="integer",
            description="Second operand for multiplication"
        ),
    }
) as dag:

    # Create ArithmeticAddCalculation
    # This TaskGroup will automatically create tasks: upload -> submit -> update -> retrieve -> parse
    add_calc = ArithmeticAddCalculation(
        group_id="addition_calc",
        computer="{{ params.computer }}",
        x="{{ params.add_x }}",
        y="{{ params.add_y }}",
        sleep=2,  # Simulate some computation time
    )

    # Create ArithmeticMultiplyCalculation
    multiply_calc = ArithmeticMultiplyCalculation(
        group_id="multiplication_calc",
        computer="{{ params.computer }}",
        x="{{ params.multiply_x }}",
        y="{{ params.multiply_y }}",
        sleep=1,
    )

    @task
    def combine_and_validate(**context):
        """
        Combine and validate results from both calculations.

        This task:
        1. Retrieves results from both CalcJobs
        2. Validates they completed successfully
        3. Combines the results
        4. Returns final output
        """
        task_instance = context['task_instance']

        # Get results from addition calc
        add_result = task_instance.xcom_pull(
            task_ids='addition_calc.parse',
            key='return_value'
        )

        # Get results from multiplication calc
        multiply_result = task_instance.xcom_pull(
            task_ids='multiplication_calc.parse',
            key='return_value'
        )

        print(f"Addition result: {add_result}")
        print(f"Multiplication result: {multiply_result}")

        # Validate both succeeded
        add_success = add_result and add_result.get('exit_status', 1) == 0
        multiply_success = multiply_result and multiply_result.get('exit_status', 1) == 0

        if not add_success:
            raise ValueError(f"Addition calculation failed: {add_result}")

        if not multiply_success:
            raise ValueError(f"Multiplication calculation failed: {multiply_result}")

        # Combine results
        combined = {
            'addition': {
                'success': add_success,
                'exit_status': add_result.get('exit_status'),
                'message': add_result.get('exit_message'),
            },
            'multiplication': {
                'success': multiply_success,
                'exit_status': multiply_result.get('exit_status'),
                'message': multiply_result.get('exit_message'),
            },
            'overall_success': True
        }

        print(f"\n{'='*60}")
        print(f"FINAL RESULTS:")
        print(f"  Addition: {add_result.get('exit_message', 'N/A')}")
        print(f"  Multiplication: {multiply_result.get('exit_message', 'N/A')}")
        print(f"  Overall: SUCCESS")
        print(f"{'='*60}\n")

        return combined

    @task
    def report_summary(**context):
        """
        Generate a summary report of all calculations.

        This demonstrates accessing CalcJob outputs for reporting.
        """
        task_instance = context['task_instance']

        combined = task_instance.xcom_pull(
            task_ids='combine_and_validate'
        )

        params = context['params']

        report = f"""
╔══════════════════════════════════════════════════════════╗
║          Arithmetic CalcJob Execution Report            ║
╚══════════════════════════════════════════════════════════╝

INPUT PARAMETERS:
  Addition:       {params['add_x']} + {params['add_y']}
  Multiplication: {params['multiply_x']} × {params['multiply_y']}

EXECUTION STATUS:
  Addition:       {combined['addition']['message']}
  Multiplication: {combined['multiplication']['message']}

OVERALL STATUS:  ✓ All calculations completed successfully

═══════════════════════════════════════════════════════════
        """

        print(report)

        return {
            'report': report,
            'timestamp': datetime.now().isoformat(),
            'status': 'SUCCESS'
        }

    # Set up task dependencies
    # Both CalcJobs run in parallel, then results are combined and reported
    combine_task = combine_and_validate()
    report_task = report_summary()

    [add_calc, multiply_calc] >> combine_task >> report_task
