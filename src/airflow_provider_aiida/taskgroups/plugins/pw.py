"""`CalcJobTaskGroup` implementation for the pw.x code of Quantum ESPRESSO."""

from pathlib import Path
from typing import Dict, Any
from aiida import orm
from aiida_quantumespresso.calculations.pw import PwCalculation as AiiDAPwCalculation

from airflow_provider_aiida.taskgroups.calcjob import CalcJobTaskGroup


class PwCalculation(CalcJobTaskGroup):
    """`CalcJobTaskGroup` implementation for the pw.x code of Quantum ESPRESSO.

    This class wraps the AiiDA PwCalculation to work with Airflow while maintaining
    backward compatibility with the AiiDA builder pattern.
    """

    def __init__(self, group_id: str, machine: str, local_workdir: str, remote_workdir: str,
                 builder=None, **kwargs):
        """
        Initialize PwCalculation task group.

        Args:
            group_id: Unique identifier for this task group
            machine: Remote machine to run on
            local_workdir: Local working directory
            remote_workdir: Remote working directory
            builder: AiiDA ProcessBuilder instance (optional)
            **kwargs: Additional arguments passed to TaskGroup
        """
        self.builder = builder
        self._inputs_dict = {}
        super().__init__(group_id, machine, local_workdir, remote_workdir, **kwargs)

    @classmethod
    def from_builder(cls, builder, group_id: str, machine: str,
                     local_workdir: str, remote_workdir: str, **kwargs):
        """Create PwCalculation from an AiiDA builder.

        This maintains backward compatibility with existing AiiDA code.
        """
        instance = cls(
            group_id=group_id,
            machine=machine,
            local_workdir=local_workdir,
            remote_workdir=remote_workdir,
            builder=builder,
            **kwargs
        )
        return instance

    def prepare(self, **context) -> Dict[str, Any]:
        """Prepare job inputs using AiiDA's input generation.

        This method uses the original AiiDA PwCalculation's input generation
        logic to create the input files and determine what needs to be uploaded.
        """
        if self.builder is None:
            raise ValueError("No builder provided to PwCalculation")

        from aiida.common.folders import Folder
        import tempfile

        # Create a temporary folder for input generation
        local_path = Path(self.local_workdir)
        local_path.mkdir(parents=True, exist_ok=True)

        # Use AiiDA's prepare_for_submission but extract the data we need
        # We'll create a temporary CalcJob instance just to generate inputs
        temp_calc = AiiDAPwCalculation(inputs=self.builder._inputs(prune=True))

        with tempfile.TemporaryDirectory() as tmpdir:
            folder = Folder(tmpdir)
            calcinfo = temp_calc.prepare_for_submission(folder)

            # Extract submission script components
            code = self.builder.code
            codeinfo = calcinfo.codes_info[0]

            # Build submission script
            submission_script = self._build_submission_script(
                code=code,
                codeinfo=codeinfo,
                calcinfo=calcinfo,
                metadata=self.builder.metadata
            )

            # Determine files to upload
            to_upload_files = {}

            # Copy generated input files from temporary folder to local workdir
            for filename in folder.get_content_list():
                src = Path(tmpdir) / filename
                if src.is_file():
                    dst = local_path / filename
                    dst.write_bytes(src.read_bytes())
                    to_upload_files[str(dst)] = filename

            # Add pseudopotential files
            for uuid, src_path, dst_path in calcinfo.local_copy_list:
                # Find the actual file from the node
                node = orm.load_node(uuid)
                # Preserve the directory structure locally (e.g., ./pseudo/Si.upf)
                local_file = local_path / Path(dst_path)

                # Create parent directory if needed
                local_file.parent.mkdir(parents=True, exist_ok=True)

                # Copy pseudo file to local workdir
                with node.open(mode='rb') as src:
                    local_file.write_bytes(src.read())
                to_upload_files[str(local_file)] = dst_path

            # Determine files to retrieve
            to_receive_files = {}
            for retrieve_path in calcinfo.retrieve_list:
                # Map remote path to local path
                if isinstance(retrieve_path, (list, tuple)):
                    remote_path = retrieve_path[0]
                else:
                    remote_path = retrieve_path
                local_file = Path(remote_path).name
                to_receive_files[remote_path] = local_file

        # Push to XCom
        context['task_instance'].xcom_push(key='to_upload_files', value=to_upload_files)
        context['task_instance'].xcom_push(key='to_receive_files', value=to_receive_files)
        context['task_instance'].xcom_push(key='submission_script', value=submission_script)

        return {
            'to_upload_files': to_upload_files,
            'to_receive_files': to_receive_files,
            'submission_script': submission_script
        }

    def _build_submission_script(self, code, codeinfo, calcinfo, metadata) -> str:
        """Build the submission script for pw.x execution with SLURM directives."""
        options = metadata.options

        # Build the command line arguments
        cmdline_params = ' '.join([f"'{param}'" for param in codeinfo.cmdline_params])

        # Get resource settings
        num_machines = options['resources'].get('num_machines', 1)
        num_mpiprocs_per_machine = options['resources'].get('num_mpiprocs_per_node',
                                                           options['resources'].get('num_mpiprocs_per_machine', 48))

        # Get walltime in format HH:MM:SS
        max_wallclock_seconds = options.get('max_wallclock_seconds', 1800)
        hours = max_wallclock_seconds // 3600
        minutes = (max_wallclock_seconds % 3600) // 60
        seconds = max_wallclock_seconds % 60
        walltime = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        # Memory in MB (convert from kB if needed)
        max_memory_kb = options.get('max_memory_kb', 490000000)  # Default ~490GB in kB
        max_memory_mb = max_memory_kb // 1024

        # Get job name (could be from metadata or generate one)
        job_name = options.get('job_name', 'aiida-pw')

        # Get the computer to access prepend text
        computer = code.computer
        prepend_text = computer.get_prepend_text()

        # Get the executable path
        executable = code.get_execname()

        # Build SLURM script matching the required format
        script = f"""#!/bin/bash
#SBATCH --no-requeue
#SBATCH --job-name="{job_name}"
#SBATCH --get-user-env
#SBATCH --output=_scheduler-stdout.txt
#SBATCH --error=_scheduler-stderr.txt
#SBATCH --nodes={num_machines}
#SBATCH --ntasks-per-node={num_mpiprocs_per_machine}
#SBATCH --time={walltime}
#SBATCH --mem={max_memory_mb}

{prepend_text}

source  /mnt/home/software/qe/7.3-ioapi/source.sh

'{executable}' {cmdline_params}  > 'aiida.out'
"""
        return script

    def parse(self, local_workdir: str, **context) -> tuple[int, Dict[str, Any]]:
        """Parse job outputs using AiiDA's parser.

        Returns:
            tuple[int, Dict[str, Any]]: (exit_status, results)
                exit_status: 0 for success, non-zero for failure/error
                results: Dictionary containing parsed results
        """
        from aiida.plugins import ParserFactory
        from aiida.common.folders import Folder
        from aiida.orm import CalcJobNode, Dict as AiiDADict

        # Load the PwParser
        PwParser = ParserFactory('quantumespresso.pw')

        local_path = Path(local_workdir)

        # Create a temporary CalcJobNode to use with the parser
        # In a real implementation, you might want to actually store this
        node = CalcJobNode()

        # The parser expects a FolderData with the retrieved files
        retrieved_folder = Folder(str(local_path))

        # Parse the outputs
        try:
            parser = PwParser(node)
            # Note: This is a simplified version. The actual parser expects
            # a retrieved FolderData node. You may need to adapt this.

            # For now, manually extract key results from the output file
            output_file = local_path / self.builder.metadata.options.output_filename

            if not output_file.exists():
                return 302, {'error': 'Output file not found'}

            # Simple parsing - in production, use the full AiiDA parser
            results = self._simple_parse_output(output_file)

            # Check for convergence
            if results.get('converged', False):
                exit_status = 0
            else:
                exit_status = 410  # ERROR_ELECTRONIC_CONVERGENCE_NOT_REACHED

            return exit_status, results

        except Exception as e:
            return 350, {'error': f'Parser exception: {str(e)}'}

    def _simple_parse_output(self, output_file: Path) -> Dict[str, Any]:
        """Simple parser for pw.x output.

        This is a minimal parser. For production, use AiiDA's full parser.
        """
        content = output_file.read_text()

        results = {
            'converged': False,
            'energy': None,
            'energy_units': 'eV',
        }

        # Check for convergence
        if 'convergence has been achieved' in content or 'End of self-consistent calculation' in content:
            results['converged'] = True

        # Extract final energy (very simplified)
        for line in content.split('\n'):
            if 'total energy' in line.lower() and '=' in line:
                try:
                    # Extract number (this is crude, improve for production)
                    parts = line.split('=')
                    if len(parts) > 1:
                        energy_str = parts[1].split()[0]
                        results['energy'] = float(energy_str)
                except (ValueError, IndexError):
                    pass

        return results
