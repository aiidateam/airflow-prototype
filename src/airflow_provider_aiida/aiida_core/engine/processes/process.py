
from airflow_provider_aiida.plumpy.process import Process
class Process(Process):

    def on_create(self) -> None:
        """Called when a Process is created."""
        super().on_create()
        # If parent PID hasn't been supplied try to get it from the stack
        if self._parent_pid is None and Process.current():
            current = Process.current()
            if isinstance(current, Process):
                self._parent_pid = current.pid  # type: ignore[assignment]
        self._pid = self._create_and_setup_db_record()
