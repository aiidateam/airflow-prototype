"""
CalcJob TaskGroup - Airflow implementation of AiiDA CalcJob interface.

This module provides a TaskGroup-based implementation that mimics the AiiDA CalcJob interface,
allowing CalcJob-style workflows to be executed in Airflow.

The interface closely follows:
- aiida.engine.processes.calcjobs.calcjob.CalcJob
- aiida.engine.processes.calcjobs.tasks (upload, submit, update, retrieve)

Uses actual AiiDA task functions from aiida.engine.processes.calcjobs.tasks
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
import tempfile
import asyncio
import sys

# Add aiida-core to path
sys.path.insert(0, '/Users/alexgo/code/airflow-provider-aiida/aiida-core-airflow-dev/src')

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

# Import AiiDA task functions
try:
    from aiida.engine.processes.calcjobs import tasks as aiida_tasks
    from aiida.engine.transports import TransportQueue
    from aiida.engine.utils import InterruptableFuture
    from aiida.orm import CalcJobNode, load_node
    from aiida.common.folders import SandboxFolder
    AIIDA_AVAILABLE = True
except ImportError as e:
    print(f"Warning: AiiDA not available: {e}")
    AIIDA_AVAILABLE = False


@dataclass
class CodeInfo:
    """
    Information about a code to execute.

    Mimics aiida.common.datastructures.CodeInfo from AiiDA.
    """
    code_uuid: Optional[str] = None
    cmdline_params: List[str] = field(default_factory=list)
    stdin_name: Optional[str] = None
    stdout_name: Optional[str] = None
    stderr_name: Optional[str] = None
    join_files: bool = False
    withmpi: Optional[bool] = None


@dataclass
class CalcInfo:
    """
    Information needed to execute a calculation job.

    Mimics aiida.common.datastructures.CalcInfo from AiiDA.
    Returned by prepare_for_submission() to specify how the job should be executed.
    """
    # UUID of the calculation (set automatically)
    uuid: Optional[str] = None

    # List of codes to run: List[CodeInfo]
    codes_info: List[CodeInfo] = field(default_factory=list)

    # Files to copy from local to remote: List[Tuple[local_abs_path, remote_rel_path, depth]]
    local_copy_list: List[tuple[str, str, str]] = field(default_factory=list)

    # Files to copy from remote to remote: List[Tuple[remote_machine_uuid, remote_abs_path, dest_rel_path]]
    remote_copy_list: List[tuple[str, str, str]] = field(default_factory=list)

    # Remote symlinks: List[Tuple[remote_machine_uuid, remote_abs_path, dest_rel_path]]
    remote_symlink_list: List[tuple[str, str, str]] = field(default_factory=list)

    # Files to retrieve after job completion: List[remote_rel_path]
    retrieve_list: List[str] = field(default_factory=list)

    # Files to retrieve temporarily (deleted after parsing): List[remote_rel_path]
    retrieve_temporary_list: List[str] = field(default_factory=list)

    # Text to prepend to submission script
    prepend_text: str = ""

    # Text to append to submission script
    append_text: str = ""

    # Skip submission (dry run mode)
    skip_submit: bool = False

    # Execution mode for multiple codes
    codes_run_mode: Optional[str] = None

    # File copy operation order
    file_copy_operation_order: Optional[List[Any]] = None


@dataclass
class ExitCode:
    """
    Exit code for a calculation job.

    Mimics aiida.engine.processes.exit_code.ExitCode from AiiDA.
    """
    status: int = 0
    message: str = ""
    invalidates_cache: bool = False

    def __bool__(self):
        """Exit code is truthy if status is non-zero (error)."""
        return self.status != 0


class Folder:
    """
    Simple folder abstraction that mimics aiida.common.folders.Folder.

    Wraps a pathlib.Path to provide compatibility with AiiDA's Folder interface.
    """
    def __init__(self, abspath: str):
        self.abspath = abspath
        self._path = Path(abspath)

    def get_abs_path(self, relpath: str = ".") -> str:
        """Get absolute path of a file/folder within this folder."""
        return str(self._path / relpath)

    def create_file_from_filelike(self, filelike, filename: str, mode: str = 'w', encoding: str = 'utf8'):
        """Create a file from a filelike object."""
        filepath = self._path / filename
        if 'b' in mode:
            with open(filepath, mode) as f:
                f.write(filelike.read())
        else:
            with open(filepath, mode, encoding=encoding) as f:
                f.write(filelike.read())

    def get_subfolder(self, subfolder: str, create: bool = False):
        """Get a subfolder, optionally creating it."""
        subfolder_path = self._path / subfolder
        if create:
            subfolder_path.mkdir(parents=True, exist_ok=True)
        return Folder(str(subfolder_path))


class CalcJob(TaskGroup, ABC):
    """
    Abstract TaskGroup for CalcJob workflows that mimics the AiiDA CalcJob interface.

    This class provides the same interface as aiida.engine.processes.calcjobs.calcjob.CalcJob:
    - prepare_for_submission(folder): Prepare calculation inputs and return CalcInfo
    - parse(retrieved_temporary_folder): Parse calculation outputs and return ExitCode

    The execution flow matches AiiDA's CalcJob lifecycle (see tasks.py):
    1. UPLOAD: presubmit() → prepare_for_submission() → upload files to remote
    2. SUBMIT: submit job to scheduler
    3. UPDATE: monitor job status until completion
    4. STASH: optionally stash files on remote
    5. RETRIEVE: retrieve output files from remote
    6. PARSE: parse() to extract results

    Key differences from fixed workdir approach:
    - Workdir is created dynamically using UUID-based sharding (like AiiDA)
    - Each task operates on the calculation node and transport
    - No fixed local_workdir/remote_workdir parameters

    Metadata options (simplified from AiiDA's full metadata.options):
    - computer: Computer/SSH connection to use
    - resources: dict with scheduler resources
    - max_wallclock_seconds: job time limit
    - queue_name: scheduler queue
    - account: scheduler account
    - parser_name: parser plugin to use (optional)
    - additional_retrieve_list: extra files to retrieve

    Exit codes (matching AiiDA):
    - 0: Success
    - 100: ERROR_NO_RETRIEVED_FOLDER
    - 110: ERROR_SCHEDULER_OUT_OF_MEMORY
    - 120: ERROR_SCHEDULER_OUT_OF_WALLTIME

    Example:
        class ArithmeticAddCalcJob(CalcJob):
            def __init__(self, x: int, y: int, **kwargs):
                super().__init__(**kwargs)
                self.x = x
                self.y = y

            def prepare_for_submission(self, folder: Folder) -> CalcInfo:
                # Write input file
                import io
                folder.create_file_from_filelike(
                    io.StringIO(f'{self.x}\\n{self.y}'),
                    'input.txt'
                )

                # Create code info
                code_info = CodeInfo()
                code_info.cmdline_params = ['bash', 'add.sh']
                code_info.stdout_name = 'output.txt'

                # Build CalcInfo
                calc_info = CalcInfo()
                calc_info.codes_info = [code_info]
                calc_info.local_copy_list = [
                    ('', folder.get_abs_path('input.txt'), 'input.txt')
                ]
                calc_info.retrieve_list = ['output.txt']

                return calc_info

            def parse(self, retrieved_temporary_folder: str) -> Optional[ExitCode]:
                # Read output
                output_file = Path(retrieved_temporary_folder) / 'output.txt'
                if not output_file.exists():
                    return self.ERROR_NO_RETRIEVED_FOLDER

                result = int(output_file.read_text())
                # Store result...

                return ExitCode(0, "Success")
    """

    # Class variables (like AiiDA)
    link_label_retrieved: str = 'retrieved'
    CACHE_VERSION: Optional[int] = None

    # Exit codes (mimicking AiiDA's standard exit codes)
    EXIT_CODE_SUCCESS = ExitCode(0, "")
    ERROR_NO_RETRIEVED_FOLDER = ExitCode(100, "The process did not have the required `retrieved` output")
    ERROR_SCHEDULER_OUT_OF_MEMORY = ExitCode(110, "The job ran out of memory")
    ERROR_SCHEDULER_OUT_OF_WALLTIME = ExitCode(120, "The job ran out of walltime")

    def __init__(
        self,
        group_id: str,
        computer: str,
        code: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        inputs: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        """
        Initialize the CalcJob task group.

        Args:
            group_id: Unique identifier for this task group
            computer: Computer/SSH connection ID (like AiiDA's Computer)
            code: Optional code identifier/path
            metadata: Optional metadata dict with options (resources, wallclock, queue, etc.)
            inputs: Optional input data nodes
            **kwargs: Additional TaskGroup arguments
        """
        super().__init__(group_id=group_id, **kwargs)
        self.computer = computer
        self.code = code
        self.metadata = metadata or {}
        self.inputs = inputs or {}

        # Outputs will be populated during execution
        self.outputs = {}

        # Remote folder will be set after upload
        self.remote_folder = None

        # Build the task group when instantiated
        self._build_tasks()

    def _build_tasks(self):
        """
        Build all tasks within this task group.

        Follows AiiDA's state machine: UPLOAD → SUBMIT → UPDATE → RETRIEVE → PARSE
        (STASH is optional and skipped in this simplified version)
        """

        # UPLOAD task - mimics task_upload_job from tasks.py
        # Calls presubmit() which calls prepare_for_submission()
        upload_task = PythonOperator(
            task_id='upload',
            python_callable=self._task_upload,
            task_group=self,
        )

        # SUBMIT task - mimics task_submit_job from tasks.py
        # Submits the job to the scheduler
        submit_task = PythonOperator(
            task_id='submit',
            python_callable=self._task_submit,
            task_group=self,
        )

        # UPDATE task - mimics task_update_job from tasks.py
        # Monitors job status until completion
        update_task = PythonOperator(
            task_id='update',
            python_callable=self._task_update,
            task_group=self,
        )

        # RETRIEVE task - mimics task_retrieve_job from tasks.py
        # Retrieves output files from remote
        retrieve_task = PythonOperator(
            task_id='retrieve',
            python_callable=self._task_retrieve,
            task_group=self,
        )

        # PARSE task - calls parse() to extract results
        parse_task = PythonOperator(
            task_id='parse',
            python_callable=self._task_parse,
            task_group=self,
        )

        # Set up dependencies matching AiiDA's state machine
        upload_task >> submit_task >> update_task >> retrieve_task >> parse_task

    def _task_upload(self, **context) -> Dict[str, Any]:
        """
        UPLOAD task - uses AiiDA's task_upload_job when available.

        This task:
        1. Creates a temporary sandbox folder
        2. Calls presubmit() which calls prepare_for_submission()
        3. Uploads files to remote machine using AiiDA task
        4. Creates remote working directory

        Returns:
            Dict with remote_folder path and calc_info
        """
        import uuid

        if AIIDA_AVAILABLE:
            # Use actual AiiDA async task
            return self._task_upload_aiida(**context)
        else:
            # Fallback to simplified implementation
            return self._task_upload_simple(**context)

    def _task_upload_simple(self, **context) -> Dict[str, Any]:
        """Simplified upload implementation without AiiDA."""
        import uuid

        # Generate UUID for this calculation
        calc_uuid = str(uuid.uuid4())

        # Create temporary sandbox folder (persists until system shutdown)
        sandbox_path = tempfile.mkdtemp(prefix=f'calcjob_sandbox_{calc_uuid}_')
        folder = Folder(sandbox_path)

        # Call presubmit (which calls prepare_for_submission)
        calc_info = self.presubmit(folder)
        calc_info.uuid = calc_uuid

        # Create remote workdir path (using UUID sharding like AiiDA)
        remote_workdir = f"/remote/work/{calc_uuid[:2]}/{calc_uuid[2:4]}/{calc_uuid[4:]}"
        self.remote_folder = remote_workdir

        # Push data to XCom for downstream tasks
        result = {
            'calc_uuid': calc_uuid,
            'remote_folder': remote_workdir,
            'sandbox_path': sandbox_path,
            'calc_info': {
                'codes_info': [
                    {
                        'cmdline_params': code_info.cmdline_params,
                        'stdin_name': code_info.stdin_name,
                        'stdout_name': code_info.stdout_name,
                        'stderr_name': code_info.stderr_name,
                    }
                    for code_info in calc_info.codes_info
                ],
                'local_copy_list': calc_info.local_copy_list,
                'remote_copy_list': calc_info.remote_copy_list,
                'retrieve_list': calc_info.retrieve_list,
                'retrieve_temporary_list': calc_info.retrieve_temporary_list,
                'prepend_text': calc_info.prepend_text,
                'append_text': calc_info.append_text,
            },
            'skip_submit': calc_info.skip_submit,
        }

        context['task_instance'].xcom_push(key='upload_result', value=result)
        return result

    def _run_async_task(self, async_func):
        """Helper to run an async function in an event loop."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No event loop running, create one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(async_func)
            finally:
                loop.close()
        else:
            # Event loop already running, use it
            return asyncio.ensure_future(async_func)

    def _create_process_adapter(self, node):
        """Create an adapter object that looks like an AiiDA CalcJob process."""
        class ProcessAdapter:
            """Adapter to make TaskGroup look like an AiiDA CalcJob process."""
            def __init__(self, calcjob_taskgroup, calcjob_node):
                self.taskgroup = calcjob_taskgroup
                self.node = calcjob_node
                self._outputs = {}

            def presubmit(self, folder):
                """Delegate to taskgroup's presubmit."""
                return self.taskgroup.presubmit(folder)

            def out(self, label, value):
                """Store output (mimics plumpy's out method)."""
                self._outputs[label] = value
                # In AiiDA, this would create an output link
                # For Airflow, we store it for later retrieval
                print(f"Process output: {label} = {value}")

        return ProcessAdapter(self, node)

    def _task_upload_aiida(self, **context) -> Dict[str, Any]:
        """Upload using actual AiiDA task_upload_job.

        This requires:
        - AiiDA database to be configured and running
        - Computer and AuthInfo setup in AiiDA
        - CalcJobNode will be created and used
        """
        from aiida.orm import load_computer
        from aiida.manage import get_manager

        # Get or create the computer
        try:
            computer = load_computer(self.computer)
        except Exception as e:
            raise ValueError(f"Could not load computer '{self.computer}': {e}. "
                           "Make sure AiiDA is configured with: verdi computer setup")

        # Create a CalcJobNode for this calculation
        node = CalcJobNode(computer=computer, process_type=self.__class__.__name__)
        node.store()

        # Create process adapter
        process_adapter = self._create_process_adapter(node)

        # Get transport queue from AiiDA manager
        manager = get_manager()
        transport_queue = manager.get_transport_queue()

        # Create cancellable future
        cancellable = InterruptableFuture()

        # Run the async upload task in event loop
        async def run_upload():
            return await aiida_tasks.task_upload_job(
                process=process_adapter,
                transport_queue=transport_queue,
                cancellable=cancellable
            )

        # Execute the async task
        skip_submit = self._run_async_task(run_upload())

        # Extract results from the adapter
        remote_folder = process_adapter._outputs.get('remote_folder')

        # Store results for XCom
        result = {
            'node_pk': node.pk,
            'node_uuid': node.uuid,
            'remote_folder': remote_folder.get_remote_path() if remote_folder else None,
            'skip_submit': skip_submit,
        }

        context['task_instance'].xcom_push(key='upload_result', value=result)
        return result

    def _task_submit(self, **context) -> Dict[str, Any]:
        """SUBMIT task - uses AiiDA's task_submit_job when available."""
        if AIIDA_AVAILABLE:
            return self._task_submit_aiida(**context)
        else:
            return self._task_submit_simple(**context)

    def _task_submit_simple(self, **context) -> Dict[str, Any]:
        """Simplified submit implementation without AiiDA."""
        upload_result = context['task_instance'].xcom_pull(
            task_ids=f'{self.group_id}.upload',
            key='upload_result'
        )

        if upload_result.get('skip_submit', False):
            return {'job_id': None, 'skipped': True}

        # Placeholder job submission
        job_id = "12345"
        result = {'job_id': job_id, 'skipped': False}
        context['task_instance'].xcom_push(key='submit_result', value=result)
        return result

    def _task_submit_aiida(self, **context) -> Dict[str, Any]:
        """Submit using actual AiiDA task_submit_job."""
        from aiida.manage import get_manager

        # Get upload result
        upload_result = context['task_instance'].xcom_pull(
            task_ids=f'{self.group_id}.upload',
            key='upload_result'
        )

        if upload_result.get('skip_submit', False):
            return {'job_id': None, 'skipped': True}

        # Load the node
        node = load_node(upload_result['node_uuid'])

        # Get transport queue
        manager = get_manager()
        transport_queue = manager.get_transport_queue()
        cancellable = InterruptableFuture()

        # Run async submit task
        async def run_submit():
            return await aiida_tasks.task_submit_job(
                node=node,
                transport_queue=transport_queue,
                cancellable=cancellable
            )

        job_id = self._run_async_task(run_submit())

        result = {'job_id': job_id, 'node_uuid': upload_result['node_uuid']}
        context['task_instance'].xcom_push(key='submit_result', value=result)
        return result

    def _task_update(self, **context) -> Dict[str, Any]:
        """UPDATE task - uses AiiDA's task_update_job when available."""
        if AIIDA_AVAILABLE:
            return self._task_update_aiida(**context)
        else:
            return self._task_update_simple(**context)

    def _task_update_simple(self, **context) -> Dict[str, Any]:
        """Simplified update implementation without AiiDA."""
        submit_result = context['task_instance'].xcom_pull(
            task_ids=f'{self.group_id}.submit',
            key='submit_result'
        )

        if submit_result.get('skipped', False):
            return {'job_done': True}

        # Placeholder: assume job is done
        job_done = True
        result = {'job_done': job_done}
        context['task_instance'].xcom_push(key='update_result', value=result)
        return result

    def _task_update_aiida(self, **context) -> Dict[str, Any]:
        """Update using actual AiiDA task_update_job."""
        from aiida.manage import get_manager

        # Get submit result
        submit_result = context['task_instance'].xcom_pull(
            task_ids=f'{self.group_id}.submit',
            key='submit_result'
        )

        if submit_result.get('skipped', False):
            return {'job_done': True}

        # Load the node
        node = load_node(submit_result['node_uuid'])

        # Get job manager
        manager = get_manager()
        job_manager = manager.get_job_manager()
        cancellable = InterruptableFuture()

        # Run async update task
        async def run_update():
            return await aiida_tasks.task_update_job(
                node=node,
                job_manager=job_manager,
                cancellable=cancellable
            )

        job_done = self._run_async_task(run_update())

        result = {'job_done': job_done, 'node_uuid': submit_result['node_uuid']}
        context['task_instance'].xcom_push(key='update_result', value=result)
        return result

    def _task_retrieve(self, **context) -> Dict[str, Any]:
        """RETRIEVE task - uses AiiDA's task_retrieve_job when available."""
        if AIIDA_AVAILABLE:
            return self._task_retrieve_aiida(**context)
        else:
            return self._task_retrieve_simple(**context)

    def _task_retrieve_simple(self, **context) -> Dict[str, Any]:
        """Simplified retrieve implementation without AiiDA."""
        import subprocess

        # Get upload result
        upload_result = context['task_instance'].xcom_pull(
            task_ids=f'{self.group_id}.upload',
            key='upload_result'
        )

        calc_info = upload_result['calc_info']
        sandbox_path = upload_result.get('sandbox_path')

        # Create temporary folder for retrieved files
        retrieved_temp_folder = tempfile.mkdtemp(prefix='aiida_retrieved_')

        # Execute calculation locally from sandbox
        if sandbox_path and Path(sandbox_path).exists():
            print(f"Executing calculation from sandbox: {sandbox_path}")

            for code_info_dict in calc_info['codes_info']:
                stdin_name = code_info_dict.get('stdin_name')
                stdout_name = code_info_dict.get('stdout_name')

                if stdin_name and stdout_name:
                    input_file = Path(sandbox_path) / stdin_name
                    output_file = Path(retrieved_temp_folder) / stdout_name

                    if input_file.exists():
                        input_content = input_file.read_text()
                        print(f"Input file {stdin_name} content: {input_content}")

                        try:
                            with open(input_file, 'r') as stdin_f, open(output_file, 'w') as stdout_f:
                                result = subprocess.run(
                                    ['bash'],
                                    stdin=stdin_f,
                                    stdout=stdout_f,
                                    stderr=subprocess.PIPE,
                                    check=True,
                                    timeout=60
                                )

                            if output_file.exists():
                                content = output_file.read_text().strip()
                                print(f"✓ Output: '{content}'")
                        except Exception as e:
                            print(f"✗ Execution failed: {e}")

        result = {
            'retrieved_folder': retrieved_temp_folder,
            'retrieve_list': calc_info['retrieve_list'],
        }
        context['task_instance'].xcom_push(key='retrieve_result', value=result)
        return result

    def _task_retrieve_aiida(self, **context) -> Dict[str, Any]:
        """Retrieve using actual AiiDA task_retrieve_job."""
        from aiida.manage import get_manager

        # Get update result
        update_result = context['task_instance'].xcom_pull(
            task_ids=f'{self.group_id}.update',
            key='update_result'
        )

        # Load the node
        node = load_node(update_result['node_uuid'])

        # Create process adapter
        process_adapter = self._create_process_adapter(node)

        # Get transport queue
        manager = get_manager()
        transport_queue = manager.get_transport_queue()
        cancellable = InterruptableFuture()

        # Create temporary folder for retrieved files
        retrieved_temp_folder = tempfile.mkdtemp(prefix='aiida_retrieved_')

        # Run async retrieve task
        async def run_retrieve():
            return await aiida_tasks.task_retrieve_job(
                process=process_adapter,
                transport_queue=transport_queue,
                retrieved_temporary_folder=retrieved_temp_folder,
                cancellable=cancellable
            )

        retrieved = self._run_async_task(run_retrieve())

        result = {
            'retrieved_folder': retrieved_temp_folder,
            'node_uuid': update_result['node_uuid'],
            'retrieved': retrieved,
        }
        context['task_instance'].xcom_push(key='retrieve_result', value=result)
        return result

    def _task_parse(self, **context) -> Dict[str, Any]:
        """
        PARSE task - calls parse() to extract results.

        This task:
        1. Gets retrieved folder path from XCom
        2. Calls user's parse() method
        3. Processes exit code
        4. Cleans up retrieved temporary folder

        Returns:
            Dict with exit status and message
        """
        # Get retrieve result from XCom
        retrieve_result = context['task_instance'].xcom_pull(
            task_ids=f'{self.group_id}.retrieve',
            key='retrieve_result'
        )

        retrieved_folder = retrieve_result['retrieved_folder']

        try:
            # Call user's parse method
            exit_code = self.parse(retrieved_folder)

            # Process exit code (default to success if None returned)
            if exit_code is None:
                exit_code = self.EXIT_CODE_SUCCESS

            # Store in XCom for downstream tasks
            result = {
                'exit_status': exit_code.status,
                'exit_message': exit_code.message,
            }

            # If exit code indicates error, raise exception
            if exit_code.status != 0:
                raise ValueError(f"CalcJob failed with exit code {exit_code.status}: {exit_code.message}")

            return result

        finally:
            # Clean up retrieved temporary folder (like AiiDA does)
            import shutil
            if Path(retrieved_folder).exists():
                shutil.rmtree(retrieved_folder, ignore_errors=True)

    def presubmit(self, folder: Folder) -> CalcInfo:
        """
        Presubmit wrapper that calls prepare_for_submission.

        INTERFACE MATCHES: aiida.engine.processes.calcjobs.calcjob.CalcJob.presubmit()

        This method:
        1. Calls prepare_for_submission() to get CalcInfo
        2. Performs validation
        3. Generates submission script from codes_info
        4. Returns CalcInfo

        In AiiDA, this also creates the job template and submission script.

        Args:
            folder: Temporary folder (SandboxFolder in AiiDA)

        Returns:
            CalcInfo object
        """
        # Call user's prepare_for_submission
        calc_info = self.prepare_for_submission(folder)

        # Validate calc_info
        if not isinstance(calc_info, CalcInfo):
            raise TypeError(f"prepare_for_submission must return CalcInfo, got {type(calc_info)}")

        # In AiiDA, this also creates the submission script and stores it in folder
        # We'll do a simplified version here

        return calc_info

    def parse_scheduler_output(self, retrieved_folder: str) -> Optional[ExitCode]:
        """
        Parse the scheduler output files to detect common scheduler errors.

        INTERFACE MATCHES: aiida.engine.processes.calcjobs.calcjob.CalcJob.parse_scheduler_output()

        This checks for scheduler-level errors like out-of-memory, walltime exceeded, etc.
        Override in subclasses to implement scheduler-specific parsing.

        Args:
            retrieved_folder: Path to folder containing retrieved files

        Returns:
            ExitCode if a scheduler error was detected, None otherwise
        """
        # Default implementation - can be overridden by subclasses
        # In AiiDA, this parses _scheduler-stdout.txt and _scheduler-stderr.txt
        return None

    @abstractmethod
    def prepare_for_submission(self, folder: Folder) -> CalcInfo:
        """
        Prepare the calculation for submission.

        INTERFACE MATCHES: aiida.engine.processes.calcjobs.calcjob.CalcJob.prepare_for_submission()

        This method should:
        1. Write input files to the folder
        2. Build and return a CalcInfo object describing:
           - codes_info: List[CodeInfo] - commands to execute
           - local_copy_list: List[Tuple] - files to upload from local
           - remote_copy_list: List[Tuple] - files to copy from another remote
           - retrieve_list: List[str] - files to retrieve after completion
           - prepend_text: str - text to add before code execution
           - append_text: str - text to add after code execution

        Args:
            folder: Temporary folder on the local file system where input files should be written
                   (aiida.common.folders.Folder equivalent)

        Returns:
            CalcInfo object containing calculation metadata

        Example:
            def prepare_for_submission(self, folder: Folder) -> CalcInfo:
                import io

                # Write input file using folder interface
                folder.create_file_from_filelike(
                    io.StringIO(f'{self.x}\\n{self.y}'),
                    'input.txt'
                )

                # Create code info
                code_info = CodeInfo()
                code_info.cmdline_params = ['bash', 'add.sh']
                code_info.stdout_name = 'output.txt'

                # Build CalcInfo
                calc_info = CalcInfo()
                calc_info.codes_info = [code_info]
                calc_info.local_copy_list = [
                    ('', folder.get_abs_path('input.txt'), 'input.txt')
                ]
                calc_info.retrieve_list = ['output.txt']

                return calc_info

        Note:
            This method is called automatically during the upload phase via presubmit().
            The folder is temporary and will be uploaded to the remote machine.
        """
        raise NotImplementedError('Subclasses must implement prepare_for_submission')

    @abstractmethod
    def parse(self, retrieved_temporary_folder: str) -> Optional[ExitCode]:
        """
        Parse the retrieved calculation outputs.

        INTERFACE MATCHES: The parsing flow from aiida.engine.processes.calcjobs.calcjob.CalcJob
        (specifically the parse method that calls parse_retrieved_output)

        This method should:
        1. Read output files from retrieved_temporary_folder
        2. Extract and validate results
        3. Store outputs (via self.outputs dict, XCom, or database)
        4. Return an ExitCode (None or ExitCode(0) for success, non-zero for errors)

        Args:
            retrieved_temporary_folder: Absolute path to temporary folder containing retrieved files
                                       (equivalent to the retrieved FolderData in AiiDA)
                                       This folder will be automatically cleaned up after parsing

        Returns:
            ExitCode object indicating success or failure:
            - None or ExitCode(0) for success
            - Non-zero status for errors

        Example:
            def parse(self, retrieved_temporary_folder: str) -> Optional[ExitCode]:
                from pathlib import Path

                output_file = Path(retrieved_temporary_folder) / 'output.txt'

                # Check if output exists
                if not output_file.exists():
                    return self.ERROR_NO_RETRIEVED_FOLDER

                # Parse output
                try:
                    result = int(output_file.read_text().strip())
                except ValueError:
                    return ExitCode(200, "Failed to parse output")

                # Store result
                self.outputs['result'] = result

                return ExitCode(0, "Success")

        Note:
            This method is called automatically after files are retrieved.
            The retrieved_temporary_folder contains all files specified in CalcInfo.retrieve_list.
            The folder is automatically deleted after this method returns.
        """
        raise NotImplementedError('Subclasses must implement parse')
