"""
Unit tests for async calcjob operators.

Run tests:
    pytest tests/operators/test_async_calcjob.py
"""

import pytest
from unittest import mock
from airflow.triggers.base import TriggerEvent

# Skip importing operators if transport module doesn't exist
pytest_plugins = []

try:
    from airflow_provider_aiida.operators.async_calcjob import (
        AsyncUploadOperator,
        AsyncSubmitOperator,
        AsyncUpdateOperator,
        AsyncReceiveOperator,
    )
    from airflow_provider_aiida.triggers.async_calcjob import (
        UploadTrigger,
        SubmitTrigger,
        UpdateTrigger,
        ReceiveTrigger,
    )
    OPERATORS_AVAILABLE = True
except ImportError as e:
    OPERATORS_AVAILABLE = False
    pytestmark = pytest.mark.skip(reason=f"Operators not available: {e}")


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    context = {
        'task_instance': mock.MagicMock()
    }
    context['task_instance'].xcom_pull.return_value = None
    return context


@pytest.mark.skipif(not OPERATORS_AVAILABLE, reason="Operators not available")
class TestAsyncUploadOperator:
    """Tests for AsyncUploadOperator."""

    def test_init(self):
        """Test operator initialization."""
        op = AsyncUploadOperator(
            task_id="test_upload",
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_upload_files={"local_file.txt": "remote_file.txt"},
        )
        assert op.machine == "localhost"
        assert op.local_workdir == "/local/path"
        assert op.remote_workdir == "/remote/path"
        assert op.to_upload_files == {"local_file.txt": "remote_file.txt"}

    def test_template_fields(self):
        """Test that template fields are correctly defined."""
        expected_fields = ["machine", "local_workdir", "remote_workdir", "to_upload_files"]
        assert AsyncUploadOperator.template_fields == expected_fields

    def test_execute_defers(self, mock_context):
        """Test that execute defers with correct trigger."""
        op = AsyncUploadOperator(
            task_id="test_upload",
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_upload_files={"local_file.txt": "remote_file.txt"},
        )

        with pytest.raises(Exception) as exc_info:
            op.execute(mock_context)

        # Verify that defer was called
        assert "defer" in str(type(exc_info.value).__name__).lower() or hasattr(exc_info.value, 'trigger')

    def test_execute_complete_success(self, mock_context):
        """Test successful completion."""
        op = AsyncUploadOperator(
            task_id="test_upload",
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_upload_files={"local_file.txt": "remote_file.txt"},
        )

        event = {"status": "success"}
        result = op.execute_complete(mock_context, event)
        assert result is None

    def test_execute_complete_error(self, mock_context):
        """Test error handling in completion."""
        op = AsyncUploadOperator(
            task_id="test_upload",
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_upload_files={"local_file.txt": "remote_file.txt"},
        )

        event = {"status": "error", "message": "Upload failed"}
        with pytest.raises(ValueError, match="Upload failed"):
            op.execute_complete(mock_context, event)


@pytest.mark.skipif(not OPERATORS_AVAILABLE, reason="Operators not available")
class TestAsyncSubmitOperator:
    """Tests for AsyncSubmitOperator."""

    def test_init(self):
        """Test operator initialization."""
        op = AsyncSubmitOperator(
            task_id="test_submit",
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            submission_script="#!/bin/bash\necho test",
        )
        assert op.machine == "localhost"
        assert op.local_workdir == "/local/path"
        assert op.remote_workdir == "/remote/path"
        assert op.submission_script == "#!/bin/bash\necho test"

    def test_template_fields(self):
        """Test that template fields are correctly defined."""
        expected_fields = ["machine", "local_workdir", "remote_workdir"]
        assert AsyncSubmitOperator.template_fields == expected_fields

    def test_execute_complete_success(self, mock_context):
        """Test successful completion returns job_id."""
        op = AsyncSubmitOperator(
            task_id="test_submit",
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            submission_script="#!/bin/bash\necho test",
        )

        event = {"status": "success", "job_id": 12345}
        result = op.execute_complete(mock_context, event)
        assert result == 12345

    def test_execute_complete_error(self, mock_context):
        """Test error handling in completion."""
        op = AsyncSubmitOperator(
            task_id="test_submit",
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            submission_script="#!/bin/bash\necho test",
        )

        event = {"status": "error", "message": "Submission failed"}
        with pytest.raises(ValueError, match="Submission failed"):
            op.execute_complete(mock_context, event)


@pytest.mark.skipif(not OPERATORS_AVAILABLE, reason="Operators not available")
class TestAsyncUpdateOperator:
    """Tests for AsyncUpdateOperator."""

    def test_init(self):
        """Test operator initialization."""
        op = AsyncUpdateOperator(
            task_id="test_update",
            machine="localhost",
            job_id=12345,
            sleep=5,
        )
        assert op.machine == "localhost"
        assert op.job_id == 12345
        assert op.sleep == 5

    def test_init_default_sleep(self):
        """Test operator initialization with default sleep value."""
        op = AsyncUpdateOperator(
            task_id="test_update",
            machine="localhost",
            job_id=12345,
        )
        assert op.sleep == 2

    def test_template_fields(self):
        """Test that template fields are correctly defined."""
        expected_fields = ["machine", "sleep"]
        assert AsyncUpdateOperator.template_fields == expected_fields

    def test_execute_complete_success(self, mock_context):
        """Test successful completion when job is done."""
        op = AsyncUpdateOperator(
            task_id="test_update",
            machine="localhost",
            job_id=12345,
        )

        event = {"status": "success", "job_completed": True}
        result = op.execute_complete(mock_context, event)
        assert result is None

    def test_execute_complete_error(self, mock_context):
        """Test error handling in completion."""
        op = AsyncUpdateOperator(
            task_id="test_update",
            machine="localhost",
            job_id=12345,
        )

        event = {"status": "error", "message": "Check failed"}
        with pytest.raises(ValueError, match="Update check failed"):
            op.execute_complete(mock_context, event)


@pytest.mark.skipif(not OPERATORS_AVAILABLE, reason="Operators not available")
class TestAsyncReceiveOperator:
    """Tests for AsyncReceiveOperator."""

    def test_init(self):
        """Test operator initialization."""
        op = AsyncReceiveOperator(
            task_id="test_receive",
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_receive_files={"remote_file.txt": "local_file.txt"},
        )
        assert op.machine == "localhost"
        assert op.local_workdir == "/local/path"
        assert op.remote_workdir == "/remote/path"
        assert op.to_receive_files == {"remote_file.txt": "local_file.txt"}

    def test_template_fields(self):
        """Test that template fields are correctly defined."""
        expected_fields = ["machine", "local_workdir", "remote_workdir", "to_receive_files"]
        assert AsyncReceiveOperator.template_fields == expected_fields

    def test_execute_complete_success(self, mock_context):
        """Test successful completion."""
        op = AsyncReceiveOperator(
            task_id="test_receive",
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_receive_files={"remote_file.txt": "local_file.txt"},
        )

        event = {"status": "success"}
        result = op.execute_complete(mock_context, event)
        assert result is None

    def test_execute_complete_error(self, mock_context):
        """Test error handling in completion."""
        op = AsyncReceiveOperator(
            task_id="test_receive",
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_receive_files={"remote_file.txt": "local_file.txt"},
        )

        event = {"status": "error", "message": "Receive failed"}
        with pytest.raises(ValueError, match="Receive failed"):
            op.execute_complete(mock_context, event)