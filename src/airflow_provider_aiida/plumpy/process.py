

from airflow_provider_aiida.plumpy.process_spec import ProcessSpec
from typing import Optional
from airflow_provider_aiida.plumpy.utils import PID_TYPE
import logging

class Process:
    _spec_class = ProcessSpec

    def __init__(
        self,
        inputs: Optional[dict] = None,
        pid: Optional[PID_TYPE] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        The signature of the constructor should not be changed by subclassing processes.

        :param inputs: A dictionary of the process inputs
        :param pid: The process ID, can be manually set, if not a unique pid will be chosen
        :param logger: An optional logger for the process to use
        :param loop: The event loop
        :param communicator: The (optional) communicator

        """
        super().__init__()

        # Don't allow the spec to be changed anymore
        self.spec().seal()

        #REMOVE self._setup_event_hooks()

        self._status: Optional[str] = None  # May hold a current status message
        #REMOVE self._pre_paused_status: Optional[str] = (
        #    None  # Save status when a pause message replaces it, such that it can be restored
        #)
        #self._paused = None

        # Input/output
        self._raw_inputs = None if inputs is None else utils.AttributesFrozendict(inputs)
        self._pid = pid
        self._parsed_inputs: Optional[utils.AttributesFrozendict] = None
        self._outputs: Dict[str, Any] = {}
        self._uuid: Optional[uuid.UUID] = None
        self._creation_time: Optional[float] = None

    @classmethod
    def spec(cls) -> ProcessSpec:
        try:
            return cls.__getattribute__(cls, '_spec')
        except AttributeError:
            try:
                cls._spec: ProcessSpec = cls._spec_class()  # type: ignore
                cls.__called: bool = False  # type: ignore
                cls.define(cls._spec)  # type: ignore
                assert cls.__called, (
                    f'Process.define() was not called by {cls}\nHint: Did you forget to call the superclass method in '
                    'your define? Try: super().define(spec)'
                )
                return cls._spec  # type: ignore
            except Exception:
                del cls._spec  # type: ignore
                cls.__called = False
                raise

    def on_create(self) -> None:
        """Entering the CREATED state."""
        import time, uuid

        self._creation_time = time.time()

        def recursively_copy_dictionaries(value: Any) -> Any:
            """Recursively copy the mapping but only create copies of the dictionaries not the values."""
            if isinstance(value, dict):
                return {key: recursively_copy_dictionaries(subvalue) for key, subvalue in value.items()}
            return value

        # This will parse the inputs with respect to the input portnamespace of the spec and validate them. The
        # ``pre_process`` method of the inputs port namespace modifies its argument in place, and since the
        # ``_raw_inputs`` should not be modified, we pass a clone of it. Note that we only need a clone of the nested
        # dictionaries, so we don't use ``copy.deepcopy`` (which might seem like the obvious choice) as that will also
        # create a clone of the values, which we don't want.
        raw_inputs = recursively_copy_dictionaries(dict(self._raw_inputs)) if self._raw_inputs else {}
        self._parsed_inputs = self.spec().inputs.pre_process(raw_inputs)
        result = self.spec().inputs.validate(self._parsed_inputs)

        if result is not None:
            raise ValueError(result)

        # Set up a process ID
        self._uuid = uuid.uuid4()
        if self._pid is None:
            self._pid = self._uuid


