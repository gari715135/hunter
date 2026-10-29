import io
import json
import logging
import sys
from pathlib import Path

import pytest

SRC_DIR = str(Path(__file__).resolve().parent.parent / "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from hunt.logger_config import ContextFilter, ContextualLogger, JsonFormatter  # noqa: E402


class JsonCapture:
    """A logger whose output is captured as parsed JSON events."""

    def __init__(self, name: str):
        self.stream = io.StringIO()
        handler = logging.StreamHandler(self.stream)
        handler.setFormatter(JsonFormatter(app="test-app", version="test"))
        handler.addFilter(ContextFilter())
        self.raw_logger = logging.getLogger(name)
        self.raw_logger.handlers = [handler]
        self.raw_logger.setLevel(logging.DEBUG)
        self.raw_logger.propagate = False
        self.logger = ContextualLogger(self.raw_logger)

    @property
    def lines(self):
        return [line for line in self.stream.getvalue().splitlines() if line]

    @property
    def events(self):
        return [json.loads(line) for line in self.lines]

    def by_event(self, name):
        return [e for e in self.events if e["event"] == name]


@pytest.fixture
def capture(request):
    return JsonCapture(f"test.{request.node.name}")
