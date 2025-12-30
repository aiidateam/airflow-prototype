[2025-12-30T14:43:27.606763Z] [SCHEDULER] info (aiida.orm.nodes.process.workflow.workchain.WorkChainNode):  | event=[464|PwBaseWorkChain|on_except]: Traceback (most recent call last):
[2025-12-30T14:43:27.606763Z] [SCHEDULER] info (aiida.orm.nodes.process.workflow.workchain.WorkChainNode):  | event=[464|PwBaseWorkChain|on_except]: Traceback (most recent call last):
  File "/home/alexgo/code/airflow-provider-aiida/.pixi/envs/default/lib/python3.11/site-packages/plumpy/processes.py", line 1334, in step
    next_state = await self._run_task(self._state.execute)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/alexgo/code/airflow-provider-aiida/.pixi/envs/default/lib/python3.11/site-packages/plumpy/processes.py", line 607, in _run_task
    result = await coro(*args, **kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/alexgo/code/airflow-provider-aiida/.pixi/envs/default/lib/python3.11/site-packages/plumpy/process_states.py", line 343, in execute
    result = await self._waiting_future
             ^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: Task <Task pending name='Task-1' coro=<ProcStepUntilTerminatedOperator._continue_run_aiida_process() running at /home/alexgo/code/airflow-provider-aiida/src/airflow_provider_aiida/operators/process.py:86>> got Future <Future pending> attached to a different loop
  File "/home/alexgo/code/airflow-provider-aiida/.pixi/envs/default/lib/python3.11/site-packages/plumpy/processes.py", line 1334, in step
    next_state = await self._run_task(self._state.execute)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/alexgo/code/airflow-provider-aiida/.pixi/envs/default/lib/python3.11/site-packages/plumpy/processes.py", line 607, in _run_task
    result = await coro(*args, **kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/alexgo/code/airflow-provider-aiida/.pixi/envs/default/lib/python3.11/site-packages/plumpy/process_states.py", line 343, in execute
    result = await self._waiting_future
             ^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: Task <Task pending name='Task-1' coro=<ProcStepUntilTerminatedOperator._continue_run_aiida_process() running at /home/alexgo/code/airflow-provider-aiida/src/airflow_provider_aiida/operators/process.py:86>> got Future <Future pending> attached to a different loop

