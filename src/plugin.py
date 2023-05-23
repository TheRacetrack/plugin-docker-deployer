from __future__ import annotations
import sys
from typing import Any

from racetrack_client.log.logs import get_logger

if 'lifecycle' in sys.modules:
    from lifecycle.deployer.infra_target import InfrastructureTarget
    from lifecycle.deployer.secrets import JobSecrets
    from deployer import DockerJobDeployer
    from monitor import DockerMonitor
    from logs_streamer import DockerLogsStreamer

    secrets_store: dict[str, JobSecrets] = {}

logger = get_logger(__name__)


class Plugin:

    def infrastructure_targets(self) -> dict[str, Any]:
        """
        Infrastructure Targets (deployment targets) for Jobs provided by this plugin
        :return dict of infrastructure name -> an instance of lifecycle.deployer.infra_target.InfrastructureTarget
        """
        return {
            'docker': InfrastructureTarget(
                job_deployer=DockerJobDeployer(secrets_store),
                job_monitor=DockerMonitor(),
                logs_streamer=DockerLogsStreamer(),
            ),
        }
