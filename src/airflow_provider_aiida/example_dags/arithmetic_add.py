###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""`CalcJob` implementation to add two numbers using bash for testing and demonstration purposes."""
from __future__ import annotations
from aiida import orm
from aiida.common.datastructures import CalcInfo, CodeInfo
from aiida.common.folders import Folder
from airflow_provider_aiida.taskgroups.calcjob import CalcJobTaskGroup

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from airflow_provider_aiida.aiida_core.engine.processes.process_spec import CalcJobProcessSpec

class ArithmeticAddCalculation(CalcJobTaskGroup):
    """`CalcJob` implementation to add two numbers using bash for testing and demonstration purposes."""

    @classmethod
    def define(cls, spec: CalcJobProcessSpec):
        """Define the process specification, including its inputs, outputs and known exit codes.

        :param spec: the calculation job process spec to define.
        """
        super().define(spec)
        spec.input('x', valid_type=(orm.Int, orm.Float), help='The left operand.')
        spec.input('y', valid_type=(orm.Int, orm.Float), help='The right operand.')
        spec.output('sum', valid_type=(orm.Int, orm.Float), help='The sum of the left and right operand.')
        spec.input('metadata.options.sleep', required=False, valid_type=int)
        # set default options (optional)
        spec.inputs['metadata']['options']['parser_name'].default = 'core.arithmetic.add'
        spec.inputs['metadata']['options']['input_filename'].default = 'aiida.in'
        spec.inputs['metadata']['options']['output_filename'].default = 'aiida.out'
        spec.inputs['metadata']['options']['resources'].default = {'num_machines': 1, 'num_mpiprocs_per_machine': 1}
        # start exit codes - marker for docs
        spec.exit_code(
            310, 'ERROR_READING_OUTPUT_FILE', invalidates_cache=True, message='The output file could not be read.'
        )
        spec.exit_code(
            320, 'ERROR_INVALID_OUTPUT', invalidates_cache=True, message='The output file contains invalid output.'
        )
        spec.exit_code(410, 'ERROR_NEGATIVE_NUMBER', message='The sum of the operands is a negative number.')
        # end exit codes - marker for docs

    def prepare_for_submission(self, folder: Folder) -> CalcInfo:
        """Prepare the calculation for submission.

        Convert the input nodes into the corresponding input files in the format that the code will expect. In addition,
        define and return a `CalcInfo` instance, which is a simple data structure that contains information for the
        engine, for example, on what files to copy to the remote machine, what files to retrieve once it has completed,
        specific scheduler settings and more.

        :param folder: a temporary folder on the local file system.
        :returns: the `CalcInfo` instance
        """
        with folder.open(self.options.input_filename, 'w', encoding='utf8') as handle:
            if 'sleep' in self.options:
                handle.write(f'sleep {self.options.sleep}\n')
            handle.write(f'echo $(({self.inputs.x.value} + {self.inputs.y.value}))\n')

        codeinfo = CodeInfo()
        codeinfo.stdin_name = self.options.input_filename
        codeinfo.stdout_name = self.options.output_filename

        if 'code' in self.inputs:
            codeinfo.code_uuid = self.inputs.code.uuid

        calcinfo = CalcInfo()
        calcinfo.codes_info = [codeinfo]
        calcinfo.retrieve_list = [self.options.output_filename]

        return calcinfo


from airflow import DAG
from airflow.models.param import Param

with DAG(
    'arithmetic_add_calcjob',
    params={
        "x": Param(8, type="integer", description="First operand for addition"),
        "y": Param(4, type="integer", description="Second operand for addition"),
        "sleep": Param(0, type="integer", description="Sleep"),
    }
) as dag:
    add_job = ArithmeticAddCalculation(
        group_id="ArithmeticAddCalculation",
        x="{{ params.x }}",
        y="{{ params.y }}",
        sleep="{{ params.y }}",
    )

    add_job


if __name__ == "__main__":
    from aiida import load_profile
    load_profile()
    """Execute the DAG for testing/debugging."""
    from datetime import datetime

    print("=" * 60)
    print("Testing arithmetic_aiida_native_single DAG")
    print("=" * 60)

    # Test the DAG with default parameters
    dag.test(
        run_conf={
            "x": 8,
            "y": 4,
            "sleep": 0,
        }
    )

    print("\n" + "=" * 60)
    print("DAG test completed!")
    print("=" * 60)
