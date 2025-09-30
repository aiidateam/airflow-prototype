"""
Unit tests for async calcjob triggers.

Run tests:
    pytest tests/triggers/test_async_calcjob.py
"""

import pytest
from unittest import mock
from pathlib import Path

from airflow_provider_aiida.triggers.async_calcjob import (
    UploadTrigger,
    SubmitTrigger,
    UpdateTrigger,
    ReceiveTrigger,
)


def create_mock_transport_queue(mock_connection):
    """Helper to create mock transport queue with correct async/await behavior."""
    async def mock_awaitable():
        return mock_connection

    mock_context = mock.MagicMock()
    mock_context.__enter__ = mock.MagicMock(return_value=mock_awaitable())
    mock_context.__exit__ = mock.MagicMock(return_value=None)

    mock_queue = mock.MagicMock()
    mock_queue.request_transport.return_value = mock_context
    return mock_queue


class TestUploadTrigger:
    """Tests for UploadTrigger."""

    def test_init(self):
        """Test trigger initialization."""
        trigger = UploadTrigger(
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_upload_files={"local_file.txt": "remote_file.txt"},
        )
        assert trigger.machine == "localhost"
        assert trigger.local_workdir == "/local/path"
        assert trigger.remote_workdir == "/remote/path"
        assert trigger.to_upload_files == {"local_file.txt": "remote_file.txt"}

    def test_serialize(self):
        """Test trigger serialization."""
        trigger = UploadTrigger(
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_upload_files={"local_file.txt": "remote_file.txt"},
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow_provider_aiida.triggers.async_calcjob.UploadTrigger"
        assert kwargs["machine"] == "localhost"
        assert kwargs["local_workdir"] == "/local/path"
        assert kwargs["remote_workdir"] == "/remote/path"
        assert kwargs["to_upload_files"] == {"local_file.txt": "remote_file.txt"}

    @pytest.mark.asyncio
    async def test_run_success(self):
        """Test successful trigger execution."""
        trigger = UploadTrigger(
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_upload_files={"local_file.txt": "remote_file.txt"},
        )

        mock_connection = mock.MagicMock()
        mock_queue = create_mock_transport_queue(mock_connection)

        with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_transport_queue", return_value=mock_queue):
            with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_authinfo_cached"):
                events = []
                async for event in trigger.run():
                    events.append(event)

                assert len(events) == 1
                assert events[0].payload["status"] == "success"

    @pytest.mark.asyncio
    async def test_run_error(self):
        """Test trigger execution with error."""
        trigger = UploadTrigger(
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_upload_files={"local_file.txt": "remote_file.txt"},
        )

        mock_queue = mock.MagicMock()
        mock_queue.request_transport.side_effect = Exception("Connection failed")

        with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_transport_queue", return_value=mock_queue):
            with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_authinfo_cached"):
                events = []
                async for event in trigger.run():
                    events.append(event)

                assert len(events) == 1
                assert events[0].payload["status"] == "error"
                assert "Connection failed" in events[0].payload["message"]


class TestSubmitTrigger:
    """Tests for SubmitTrigger."""

    def test_init(self):
        """Test trigger initialization."""
        trigger = SubmitTrigger(
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            submission_script="#!/bin/bash\necho test",
        )
        assert trigger.machine == "localhost"
        assert trigger.local_workdir == "/local/path"
        assert trigger.remote_workdir == "/remote/path"
        assert trigger.submission_script == "#!/bin/bash\necho test"

    def test_serialize(self):
        """Test trigger serialization."""
        trigger = SubmitTrigger(
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            submission_script="#!/bin/bash\necho test",
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow_provider_aiida.triggers.async_calcjob.SubmitTrigger"
        assert kwargs["machine"] == "localhost"
        assert kwargs["submission_script"] == "#!/bin/bash\necho test"

    @pytest.mark.asyncio
    async def test_run_success(self, tmp_path):
        """Test successful trigger execution."""
        local_dir = tmp_path / "local"
        local_dir.mkdir()

        trigger = SubmitTrigger(
            machine="localhost",
            local_workdir=str(local_dir),
            remote_workdir="/remote/path",
            submission_script="#!/bin/bash\necho test",
        )

        mock_connection = mock.MagicMock()
        mock_connection.exec_command_wait.return_value = (0, "12345", "")
        mock_queue = create_mock_transport_queue(mock_connection)

        with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_transport_queue", return_value=mock_queue):
            with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_authinfo_cached"):
                events = []
                async for event in trigger.run():
                    events.append(event)

                assert len(events) == 1
                assert events[0].payload["status"] == "success"
                assert events[0].payload["job_id"] == 12345

    @pytest.mark.asyncio
    async def test_run_submission_failed(self, tmp_path):
        """Test trigger execution with submission failure."""
        local_dir = tmp_path / "local"
        local_dir.mkdir()

        trigger = SubmitTrigger(
            machine="localhost",
            local_workdir=str(local_dir),
            remote_workdir="/remote/path",
            submission_script="#!/bin/bash\necho test",
        )

        mock_connection = mock.MagicMock()
        mock_connection.exec_command_wait.return_value = (1, "", "Error submitting job")
        mock_queue = create_mock_transport_queue(mock_connection)

        with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_transport_queue", return_value=mock_queue):
            with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_authinfo_cached"):
                events = []
                async for event in trigger.run():
                    events.append(event)

                assert len(events) == 1
                assert events[0].payload["status"] == "error"
                assert "Submission did not work" in events[0].payload["message"]


class TestUpdateTrigger:
    """Tests for UpdateTrigger."""

    def test_init(self):
        """Test trigger initialization."""
        trigger = UpdateTrigger(
            machine="localhost",
            job_id=12345,
            sleep=5,
        )
        assert trigger.machine == "localhost"
        assert trigger.job_id == 12345
        assert trigger.sleep == 5

    def test_init_default_sleep(self):
        """Test trigger initialization with default sleep."""
        trigger = UpdateTrigger(
            machine="localhost",
            job_id=12345,
        )
        assert trigger.sleep == 2

    def test_serialize(self):
        """Test trigger serialization."""
        trigger = UpdateTrigger(
            machine="localhost",
            job_id=12345,
            sleep=5,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow_provider_aiida.triggers.async_calcjob.UpdateTrigger"
        assert kwargs["machine"] == "localhost"
        assert kwargs["job_id"] == 12345
        assert kwargs["sleep"] == 5

    @pytest.mark.asyncio
    async def test_run_job_completed_immediately(self):
        """Test trigger when job is already completed."""
        trigger = UpdateTrigger(
            machine="localhost",
            job_id=12345,
            sleep=1,
        )

        mock_connection = mock.MagicMock()
        mock_connection.exec_command_wait.return_value = (1, "", "")
        mock_queue = create_mock_transport_queue(mock_connection)

        with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_transport_queue", return_value=mock_queue):
            with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_authinfo_cached"):
                events = []
                async for event in trigger.run():
                    events.append(event)

                assert len(events) == 1
                assert events[0].payload["status"] == "success"
                assert events[0].payload["job_completed"] is True

    @pytest.mark.asyncio
    async def test_run_job_running_then_completes(self):
        """Test trigger when job is running then completes."""
        trigger = UpdateTrigger(
            machine="localhost",
            job_id=12345,
            sleep=0.1,
        )

        mock_connection = mock.MagicMock()
        mock_connection.exec_command_wait.side_effect = [
            (0, "", ""),  # Job running
            (1, "", ""),  # Job completed
        ]
        mock_queue = create_mock_transport_queue(mock_connection)

        with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_transport_queue", return_value=mock_queue):
            with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_authinfo_cached"):
                events = []
                async for event in trigger.run():
                    events.append(event)

                assert len(events) == 1
                assert events[0].payload["status"] == "success"
                assert events[0].payload["job_completed"] is True

    @pytest.mark.asyncio
    async def test_run_error(self):
        """Test trigger execution with error."""
        trigger = UpdateTrigger(
            machine="localhost",
            job_id=12345,
        )

        mock_queue = mock.MagicMock()
        mock_queue.request_transport.side_effect = Exception("Connection failed")

        with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_transport_queue", return_value=mock_queue):
            with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_authinfo_cached"):
                events = []
                async for event in trigger.run():
                    events.append(event)

                assert len(events) == 1
                assert events[0].payload["status"] == "error"
                assert "Connection failed" in events[0].payload["message"]


class TestReceiveTrigger:
    """Tests for ReceiveTrigger."""

    def test_init(self):
        """Test trigger initialization."""
        trigger = ReceiveTrigger(
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_receive_files={"remote_file.txt": "local_file.txt"},
        )
        assert trigger.machine == "localhost"
        assert trigger.local_workdir == "/local/path"
        assert trigger.remote_workdir == "/remote/path"
        assert trigger.to_receive_files == {"remote_file.txt": "local_file.txt"}

    def test_serialize(self):
        """Test trigger serialization."""
        trigger = ReceiveTrigger(
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_receive_files={"remote_file.txt": "local_file.txt"},
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow_provider_aiida.triggers.async_calcjob.ReceiveTrigger"
        assert kwargs["machine"] == "localhost"
        assert kwargs["local_workdir"] == "/local/path"
        assert kwargs["remote_workdir"] == "/remote/path"
        assert kwargs["to_receive_files"] == {"remote_file.txt": "local_file.txt"}

    @pytest.mark.asyncio
    async def test_run_success(self):
        """Test successful trigger execution."""
        trigger = ReceiveTrigger(
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_receive_files={"remote_file.txt": "local_file.txt"},
        )

        mock_connection = mock.MagicMock()
        mock_queue = create_mock_transport_queue(mock_connection)

        with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_transport_queue", return_value=mock_queue):
            with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_authinfo_cached"):
                events = []
                async for event in trigger.run():
                    events.append(event)

                assert len(events) == 1
                assert events[0].payload["status"] == "success"

    @pytest.mark.asyncio
    async def test_run_error(self):
        """Test trigger execution with error."""
        trigger = ReceiveTrigger(
            machine="localhost",
            local_workdir="/local/path",
            remote_workdir="/remote/path",
            to_receive_files={"remote_file.txt": "local_file.txt"},
        )

        mock_queue = mock.MagicMock()
        mock_queue.request_transport.side_effect = Exception("Connection failed")

        with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_transport_queue", return_value=mock_queue):
            with mock.patch("airflow_provider_aiida.triggers.async_calcjob.get_authinfo_cached"):
                events = []
                async for event in trigger.run():
                    events.append(event)

                assert len(events) == 1
                assert events[0].payload["status"] == "error"
                assert "Connection failed" in events[0].payload["message"]
