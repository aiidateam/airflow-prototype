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
from aiida.engine.processes.calcjobs.calcjob import CalcJob
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation

from airflow import DAG
from airflow.models.param import Param

with DAG(
    'arithmetic_add_calcjob',
    params={
        "x": Param(8, type="integer", description="First operand for addition"),
        "y": Param(4, type="integer", description="Second operand for addition"),
        "code": Param("bash@localhost", type="string"),
        "metadata": {"options": {"sleep": 0}},
    }
) as dag:
    from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
    print(ArithmeticAddCalculation.__module__)
    add_job = CalcJobTaskGroup(
        group_id="ArithmeticAddCalculation",
        process_class=ArithmeticAddCalculation,
        inputs = dict(x= "{{ params.x }}",
                      y="{{ params.y }}",
                      metadata="{{ params.metadata }}",
                )
    )

    add_job


if __name__ == "__main__":
    from aiida import load_profile
    load_profile()

    print("=" * 60)
    print("Testing arithmetic_aiida_native_single DAG")
    print("=" * 60)

    # Test the DAG with default parameters
    dag.test(
        run_conf={
            "x": 8,
            "y": 4,
            "metadata": {"options": {"sleep": 0}},
            "code": "bash@localhost"
        }
    )

    print("\n" + "=" * 60)
    print("DAG test completed!")
    print("=" * 60)
