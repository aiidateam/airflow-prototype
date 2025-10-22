from airflow_provider_aiida.plumpy.process_spec import ProcessSpec as PlumpyProcessSpec
from airflow_provider_aiida.plumpy.ports import InputPort, PortNamespace
from aiida.engine.processes.exit_code import ExitCodesNamespace, ExitCode
from typing import Optional

class ProcessSpec(PlumpyProcessSpec):
    """Default process spec for process classes defined in `aiida-core`.

    This sub class defines custom classes for input ports and port namespaces. It also adds support for the definition
    of exit codes and retrieving them subsequently.
    """

    METADATA_KEY: str = 'metadata'
    METADATA_OPTIONS_KEY: str = 'options'
    INPUT_PORT_TYPE = InputPort
    PORT_NAMESPACE_TYPE = PortNamespace

    def __init__(self) -> None:
        super().__init__()
        self._exit_codes = ExitCodesNamespace()

    @property
    def metadata_key(self) -> str:
        return self.METADATA_KEY

    @property
    def options_key(self) -> str:
        return self.METADATA_OPTIONS_KEY

    @property
    def exit_codes(self) -> ExitCodesNamespace:
        """Return the namespace of exit codes defined for this ProcessSpec

        :returns: ExitCodesNamespace of ExitCode named tuples
        """
        return self._exit_codes

    def exit_code(self, status: int, label: str, message: str, invalidates_cache: bool = False) -> None:
        """Add an exit code to the ProcessSpec

        :param status: the exit status integer
        :param label: a label by which the exit code can be addressed
        :param message: a more detailed description of the exit code
        :param invalidates_cache: when set to `True`, a process exiting
            with this exit code will not be considered for caching
        """
        if not isinstance(status, int):
            raise TypeError(f'status should be of integer type and not of {type(status)}')

        if status < 0:
            raise ValueError(f'status should be a positive integer, received {type(status)}')

        if not isinstance(label, str):
            raise TypeError(f'label should be of str type and not of {type(label)}')

        if not isinstance(message, str):
            raise TypeError(f'message should be of str type and not of {type(message)}')

        if not isinstance(invalidates_cache, bool):
            raise TypeError(f'invalidates_cache should be of type bool and not of {type(invalidates_cache)}')

        self._exit_codes[label] = ExitCode(status, message, invalidates_cache=invalidates_cache)

    # override return type to aiida's PortNamespace subclass

    @property
    def ports(self) -> PortNamespace:
        return super().ports  # type: ignore[return-value]

    @property
    def inputs(self) -> PortNamespace:
        return super().inputs  # type: ignore[return-value]

    @property
    def outputs(self) -> PortNamespace:
        return super().outputs  # type: ignore[return-value]


from airflow_provider_aiida.aiida_core.engine.processes import ports


class CalcJobOutputPort(ports.OutputPort):
    """Sub class of plumpy.OutputPort which adds the `_pass_to_parser` attribute."""

    def __init__(self, *args, **kwargs) -> None:
        pass_to_parser = kwargs.pop('pass_to_parser', False)
        super().__init__(*args, **kwargs)
        self._pass_to_parser: bool = pass_to_parser

    @property
    def pass_to_parser(self) -> bool:
        return self._pass_to_parser

class CalcJobProcessSpec(ProcessSpec):
    """Process spec intended for the `CalcJob` process class."""

    OUTPUT_PORT_TYPE = CalcJobOutputPort

    def __init__(self) -> None:
        super().__init__()
        self._default_output_node: Optional[str] = None

    @property
    def default_output_node(self) -> Optional[str]:
        return self._default_output_node

    @default_output_node.setter
    def default_output_node(self, port_name: str) -> None:
        if port_name not in self.outputs:
            raise ValueError(f'{port_name} is not a registered output port')

        valid_type_port = self.outputs[port_name].valid_type
        valid_type_required = Dict

        if valid_type_port is not valid_type_required:
            raise ValueError(
                f'the valid type of a default output has to be a {valid_type_required} but it is {valid_type_port}'
            )

        self._default_output_node = port_name
