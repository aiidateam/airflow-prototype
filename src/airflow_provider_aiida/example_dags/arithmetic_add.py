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
from airflow_provider_aiida.taskgroups.process import ProcessTaskGroup 
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation

from airflow import DAG
from airflow.models.param import Param

with DAG(
    'ArithmeticAddCalculation',
    params={
        "node_pk": Param("", type="integer")
    },
    render_template_as_native_obj = True
) as dag:
    add_job = ProcessTaskGroup(
        process_class=ArithmeticAddCalculation,
        node_pk="{{ params.node_pk }}",
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
