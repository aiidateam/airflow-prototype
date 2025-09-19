"""
Custom triggers for async file operations.
This module is in plugins/ so Airflow can properly import and deserialize triggers.
"""

import asyncio
import time
from pathlib import Path
from typing import Any

from airflow.triggers.base import TriggerEvent, BaseTrigger


class AsyncFileWriteTrigger(BaseTrigger):
    """Custom trigger that performs async file write operations."""

    def __init__(self, file_path: str, content: str, delay: float = 0.1):
        super().__init__()
        self.file_path = file_path
        self.content = content
        self.delay = delay

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "async_file_triggers.AsyncFileWriteTrigger",
            {
                "file_path": self.file_path,
                "content": self.content,
                "delay": self.delay,
            },
        )

    async def run(self):
        """Perform async file write with artificial delay."""
        start_time = time.time()

        # Simulate async I/O operation
        await asyncio.sleep(self.delay)

        # Write file asynchronously
        path = Path(self.file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Use aiofiles for truly async file operations
        try:
            import aiofiles
            async with aiofiles.open(self.file_path, 'w') as f:
                await f.write(self.content)
        except ImportError:
            # Fallback to sync write if aiofiles not available
            with open(self.file_path, 'w') as f:
                f.write(self.content)

        end_time = time.time()

        yield TriggerEvent({
            "status": "success",
            "file_path": self.file_path,
            "execution_time": end_time - start_time,
            "content_length": len(self.content),
        })