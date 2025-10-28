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
        #"x": Param(8, type="integer", description="First operand for addition"),
        #"y": Param(4, type="integer", description="Second operand for addition"),
        #"code": Param("bash@localhost", type="string"),
        #"metadata": {"options": {"sleep": 0}},
        #"node_pk": Param(None, type=["integer", "null"])
        "node_pk": Param("", type="integer")
    },
    render_template_as_native_obj = True
) as dag:
    add_job = CalcJobTaskGroup(
        process_class=ArithmeticAddCalculation,
        node_pk="{{ params.node_pk }}",
        #inputs = dict(x="{{ params.x }}",
        #              y="{{ params.y }}",
        #              metadata="{{ params.metadata }}",
        #        )
    )

    add_job


if __name__ == "__main__":
    from aiida import load_profile
    load_profile()

    print("=" * 60)
    print("Testing arithmetic_aiida_native_single DAG")
    print("=" * 60)

    # Test the DAG with default parameters
    from aiida.orm import load_code, Int
    code = load_code('bash@localhost')
    inputs = {
        'code': code,
        'x': Int(0),
        'y': Int(1),
        #'metadata': {'options': {'sleep': 5}} 
    }
    
    process = ArithmeticAddCalculation(inputs=inputs)
    # For creating pesistence checkpoints and other database related actions
    process._save_checkpoint()

    dag.test(
        run_conf={
            #"x": 8,
            #"y": 4,
            #"metadata": {"options": {"sleep": 0}},
            #"code": "bash@localhost"
            "node_pk": process.node.pk
        }
    )

    print("\n" + "=" * 60)
    print("DAG test completed!")
    print("=" * 60)
