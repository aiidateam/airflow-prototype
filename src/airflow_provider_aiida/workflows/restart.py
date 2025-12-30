"""
Restart workchain for testing and debugging.

This module provides a simple workchain that extends BaseRestartWorkChain
for testing error handling and restart behavior.
"""
from aiida import orm
from aiida.engine import BaseRestartWorkChain, while_, process_handler, ProcessHandlerReport


class ArithmeticRestartWorkChain(BaseRestartWorkChain):
    """
    A restart workchain for arithmetic addition using ArithmeticAddCalculation.

    This workchain demonstrates the BaseRestartWorkChain error handling mechanism
    by retrying ArithmeticAddCalculation if it fails.
    """

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input('code', valid_type=orm.AbstractCode, required=False,
                  help='Code for arithmetic.add calculation')
        spec.input('x', valid_type=orm.Int, help='First integer')
        spec.input('y', valid_type=orm.Int, help='Second integer')
        spec.input('fail_threshold', valid_type=orm.Float, default=lambda: orm.Float(0.0),
                  help='Probability of random failure for testing (0.0-1.0)')
        spec.output('result', valid_type=orm.Int, required=False)

        # Set the process class
        from aiida.plugins import CalculationFactory
        cls._process_class = CalculationFactory('core.arithmetic.add')

        # Define the outline
        spec.outline(
            cls.setup,
            while_(cls.should_run_process)(
                cls.run_process,
                cls.inspect_process,
            ),
            cls.results,
        )

    def setup(self):
        """Call the parent setup and add any additional setup."""
        super().setup()
        self.ctx.inputs = {
            'x': self.inputs.x,
            'y': self.inputs.y,
        }
        if 'code' in self.inputs:
            self.ctx.inputs['code'] = self.inputs.code

    @process_handler(priority=100)
    def handle_random_failure(self, node):
        """Process handler that randomly injects failures for testing.

        This handler is called after each calculation completes. It randomly
        decides to inject a failure based on the fail_threshold input to test
        the restart mechanism.
        """
        import random

        # Only inject failures if the calculation succeeded
        if not node.is_finished_ok:
            return None

        # Randomly inject failure for testing based on fail_threshold
        fail_threshold = self.inputs.fail_threshold.value
        if random.random() < fail_threshold:
            self.report(f'Randomly injecting failure for testing (threshold={fail_threshold}, iteration={self.ctx.iteration})')
            # Return a report with exit code 0 to signal restart
            from aiida.engine import ExitCode
            return ProcessHandlerReport(exit_code=ExitCode(0))

        # No failure injection - let the calculation succeed
        # Store iteration count in extras
        self.node.base.extras.set('iteration', self.ctx.iteration)
        return None

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
