from typing import Callable

from lifecycle.monitor.base import LogsStreamer
from racetrack_client.log.logs import get_logger
from racetrack_client.utils.shell import CommandOutputStream, CommandError
from racetrack_commons.deploy.resource import job_resource_name

logger = get_logger(__name__)


class DockerLogsStreamer(LogsStreamer):
    """Source of a Job logs retrieved from a Docker container"""

    def __init__(self):
        super().__init__()
        self.sessions: dict[str, CommandOutputStream] = {}

    def create_session(self, session_id: str, resource_properties: dict[str, str], on_next_line: Callable[[str, str], None]):
        """Start a session transmitting messages to a client."""
        job_name = resource_properties.get('job_name')
        job_version = resource_properties.get('job_version')
        tail = resource_properties.get('tail')
        container_name = job_resource_name(job_name, job_version)

        def on_next_session_line(line: str):
            on_next_line(session_id, line)

        def on_error(error: CommandError):
            # Negative return value is the signal number which was used to kill the process. SIGTERM is 15.
            if error.returncode != -15:  # ignore process Terminated on purpose
                logger.error(f'command "{error.cmd}" failed with return code {error.returncode}')

        cmd = f'docker logs "{container_name}" --follow --tail {tail}'
        output_stream = CommandOutputStream(cmd, on_next_line=on_next_session_line, on_error=on_error)
        self.sessions[session_id] = output_stream

    def close_session(self, session_id: str):
        output_stream = self.sessions[session_id]
        output_stream.interrupt()
        del self.sessions[session_id]
