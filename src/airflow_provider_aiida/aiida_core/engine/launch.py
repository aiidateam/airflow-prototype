from airflow_provider_aiida.aiida_core.engine.runner import AirflowRunner
from aiida.engine.utils import instantiate_process, is_process_scoped, prepare_inputs
from aiida.engine.processes.functions import FunctionProcess
from aiida.engine.processes.process import Process
from aiida.engine.processes.builder import ProcessBuilder

from aiida.common import InvalidOperation
from aiida.orm import ProcessNode
import logging
import time
import typing as t

from airflow.api.client import get_current_api_client

if t.TYPE_CHECKING:
    from aiida.engine.runners import ResultAndPk


LOGGER = logging.getLogger(__name__)

TYPE_RUN_PROCESS = t.Union[Process, t.Type[Process], ProcessBuilder]

def submit(
    process,
    inputs: dict[str, t.Any] | None = None,
    *,
    wait: bool = False,
    wait_interval: int = 5,
    **kwargs: t.Any,
) -> ProcessNode:
    """Submit the process with the supplied inputs to the daemon immediately returning control to the interpreter.

    .. warning: this should not be used within another process. Instead, there one should use the ``submit`` method of
        the wrapping process itself, i.e. use ``self.submit``.

    .. warning: submission of processes requires ``store_provenance=True``.

    :param process: the process class, instance or builder to submit
    :param inputs: the input dictionary to be passed to the process
    :param wait: when set to ``True``, the submission will be blocking and wait for the process to complete at which
        point the function returns the calculation node.
    :param wait_interval: the number of seconds to wait between checking the state of the process when ``wait=True``.
    :param kwargs: inputs to be passed to the process. This is an alternative to the positional ``inputs`` argument.
    :return: the calculation node of the process
    """

    inputs = prepare_inputs(inputs, **kwargs)

    # Submitting from within another process requires ``self.submit``` unless it is a work function, in which case the
    # current process in the scope should be an instance of ``FunctionProcess``.
    if is_process_scoped() and not isinstance(Process.current(), FunctionProcess):
        raise InvalidOperation('Cannot use top-level `submit` from within another process, use `self.submit` instead')

    runner = AirflowRunner(broker_submit=True)

    assert runner.persister is not None, 'runner does not have a persister'

    process_inited = instantiate_process(runner, process, **inputs)

    # If a dry run is requested, simply forward to `run`, because it is not compatible with `submit`. We choose for this
    # instead of raising, because in this way the user does not have to change the launcher when testing. The same goes
    # for if `remote_folder` is present in the inputs, which means we are importing an already completed calculation.
    if process_inited.metadata.get('dry_run', False) or 'remote_folder' in inputs:
        _, node = run_get_node(process_inited)
        return node

    if not process_inited.metadata.store_provenance:
        raise InvalidOperation('cannot submit a process with `store_provenance=False`')

    runner.persister.save_checkpoint(process_inited)
    process_inited.close()
    node = process_inited.node

    # Do not wait for the future's result, because in the case of a single worker this would cock-block itself
    get_current_api_client().trigger_dag(
        dag_id=process_inited.__class__.__name__,
        conf={"node_pk": node.pk}
    )

    if not wait:
        return node

    while not node.is_terminated:
        LOGGER.report(
            f'Process<{node.pk}> has not yet terminated, current state is `{node.process_state}`. '
            f'Waiting for {wait_interval} seconds.'
        )
        time.sleep(wait_interval)

    return node

def run(process: TYPE_RUN_PROCESS, inputs: dict[str, t.Any] | None = None, **kwargs: t.Any) -> dict[str, t.Any]:
    raise NotImplementedError()

def run_get_pk(process: TYPE_RUN_PROCESS, inputs: dict[str, t.Any] | None = None, **kwargs: t.Any) -> 'ResultAndPk':
    raise NotImplementedError()

def run_get_node(
    process: TYPE_RUN_PROCESS, inputs: dict[str, t.Any] | None = None, **kwargs: t.Any
) -> tuple[dict[str, t.Any], ProcessNode]:
    """Run the process with the supplied inputs in a local runner that will block until the process is completed.

    :param process: the process class, instance, builder or function to run
    :param inputs: the inputs to be passed to the process
    :return: tuple of the outputs of the process and the process node
    """
    if isinstance(process, Process):
        if process.runner is None:
            process.runner = AirflowRunner(broker_submit=False)
            runner = process.runner
        else:
            runner = process.runner
            # this case is to give a proper error message for backwards usage
            assert isinstance(runner, AirflowRunner)
            assert not runner.broker_submit
    else:
        runner = AirflowRunner(broker_submit=False)

    return runner.run_get_node(process, inputs, **kwargs)
