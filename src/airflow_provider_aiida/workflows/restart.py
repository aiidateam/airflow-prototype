"""
Restart workchain for testing and debugging.

This module provides a simple workchain that extends BaseRestartWorkChain
for testing error handling and restart behavior.
"""
from aiida import orm
from aiida.engine import BaseRestartWorkChain, while_, process_handler, ProcessHandlerReport
from aiida.plugins import CalculationFactory


class ArithmeticRestartWorkChain(BaseRestartWorkChain):
    """
    A restart workchain for arithmetic addition using ArithmeticAddCalculation.

    This workchain demonstrates the BaseRestartWorkChain error handling mechanism
    by retrying ArithmeticAddCalculation if it fails.
    """

    _process_class = CalculationFactory('core.arithmetic.add') 

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input('code', valid_type=orm.AbstractCode, required=False,
                  help='Code for arithmetic.add calculation')
        spec.input('x', valid_type=orm.Int, help='First integer')
        spec.input('y', valid_type=orm.Int, help='Second integer')
        spec.input('num_failures', valid_type=orm.Int, default=lambda: orm.Int(0),
                  help='Number of times to inject failures before succeeding (for testing)')
        spec.output('result', valid_type=orm.Int, required=False)

        # Set the process class

        # Define the outline
        spec.outline(
            cls.setup,
            while_(cls.should_run_process)(
                cls.run_process,
                cls.inspect_process,
            ),
            cls.results,
        )
        #spec.expose_inputs(cls._process_class)
        spec.expose_outputs(cls._process_class)

    def setup(self):
        """Call the parent setup and add any additional setup."""
        super().setup()
        self.ctx.inputs = {
            'x': self.inputs.x,
            'y': self.inputs.y,
        }
        if 'code' in self.inputs:
            self.ctx.inputs['code'] = self.inputs.code

        # Initialize failure counter for testing
        self.ctx.num_failures_injected = 0

    @process_handler(priority=100)
    def handle_injected_failure(self, node):
        """Process handler that injects a specific number of failures for testing.

        This handler is called after each calculation completes. It injects
        exactly num_failures failures before allowing the calculation to succeed.
        """
        # Only inject failures if the calculation succeeded
        if not node.is_finished_ok:
            return None

        num_failures = self.inputs.num_failures.value
        # Check if we should inject another failure
        if self.ctx.num_failures_injected < num_failures:
            self.ctx.num_failures_injected += 1
            self.report(f'Injecting failure {self.ctx.num_failures_injected}/{num_failures} for testing (iteration={self.ctx.iteration})')
            # Return a report with exit code 0 to signal restart
            from aiida.engine import ExitCode
            return ProcessHandlerReport(exit_code=ExitCode(0))

        # No more failures to inject - let the calculation succeed
        # Store iteration count in extras
        self.node.base.extras.set('iteration', self.ctx.iteration)
        return None

    def get_outputs(self, node):
        """Override to add debug logging."""
        self.report(f'DEBUG: get_outputs() called for node {node.pk}')
        self.report(f'DEBUG: self.process_class = {self.process_class}')
        self.report(f'DEBUG: self._process_class = {self._process_class}')

        # Log what's in the spec
        self.report(f'DEBUG: spec()._exposed_outputs keys: {list(self.spec()._exposed_outputs.keys())}')
        for ns, proc_dict in self.spec()._exposed_outputs.items():
            self.report(f'DEBUG:   Namespace {ns}: process_classes = {list(proc_dict.keys())}')
            for pc, outputs in proc_dict.items():
                self.report(f'DEBUG:     {pc}: {outputs}')
                self.report(f'DEBUG:     Is it self.process_class? {pc is self.process_class}')

        # Log what outputs the node actually has
        from aiida.common import LinkType
        node_outputs = node.base.links.get_outgoing(link_type=(LinkType.CREATE, LinkType.RETURN)).nested()
        self.report(f'DEBUG: Node {node.pk} has outputs: {list(node_outputs.keys())}')

        # Call the parent method
        result = super().get_outputs(node)
        self.report(f'DEBUG: exposed_outputs() returned: {list(result.keys())}')
        return result

    def results(self):
        """Attach the outputs of the last successful calculation."""
        # Call parent results() first - it handles max iterations check
        result = super().results()
        if result is not None:
            # Parent returned an exit code (e.g., max iterations exceeded)
            return result

        # Success - attach outputs
        calculation = self.ctx.children[-1]

        # The ArithmeticAddCalculation outputs 'sum', but we expose it as 'result'
        if 'sum' in calculation.outputs:
            self.out('result', calculation.outputs.sum)
        else:
            self.report('Warning: calculation completed but has no sum output')

        return None
