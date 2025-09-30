"""
Unit tests for calcjob operators.

Run tests:
    pytest tests/operators/test_calcjob.py
"""

import pytest

from airflow_provider_aiida.operators.calcjob import (
    UploadOperator,
    SubmitOperator,
    UpdateOperator,
    ReceiveOperator,
)


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    class MockTaskInstance:
        def xcom_pull(self, task_ids, key):
            return {}

    return {'task_instance': MockTaskInstance()}


class TestUploadOperator:
    """Tests for UploadOperator."""

    def test_init(self):
        """Test operator initialization."""
        op = UploadOperator(
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
        assert UploadOperator.template_fields == expected_fields


class TestSubmitOperator:
    """Tests for SubmitOperator."""

    def test_init(self):
        """Test operator initialization."""
        op = SubmitOperator(
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
        assert SubmitOperator.template_fields == expected_fields


class TestUpdateOperator:
    """Tests for UpdateOperator."""

    def test_init(self):
        """Test operator initialization."""
        op = UpdateOperator(
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
        op = UpdateOperator(
            task_id="test_update",
            machine="localhost",
            job_id=12345,
        )
        assert op.sleep == 2

    def test_template_fields(self):
        """Test that template fields are correctly defined."""
        expected_fields = ["machine", "sleep"]
        assert UpdateOperator.template_fields == expected_fields


class TestReceiveOperator:
    """Tests for ReceiveOperator."""

    def test_init(self):
        """Test operator initialization."""
        op = ReceiveOperator(
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
        assert ReceiveOperator.template_fields == expected_fields