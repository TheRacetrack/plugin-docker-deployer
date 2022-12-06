from __future__ import annotations
from typing import Any

from racetrack_client.log.logs import get_logger

logger = get_logger(__name__)

try:
    from lifecycle.deployer.infra_target import InfrastructureTarget
    from deployer import DockerFatmanDeployer
    from monitor import DockerMonitor
    from logs_streamer import DockerLogsStreamer
except ModuleNotFoundError:
    logger.debug('Skipping Lifecycle\'s imports')


class Plugin:

    def infrastructure_targets(self) -> dict[str, Any]:
        """
        Infrastracture Targets (deployment targets) for Fatmen provided by this plugin
        :return dict of infrastructure name -> an instance of lifecycle.deployer.infra_target.InfrastructureTarget
        """
        return {
            'docker': InfrastructureTarget(
                fatman_deployer=DockerFatmanDeployer(),
                fatman_monitor=DockerMonitor(),
                logs_streamer=DockerLogsStreamer(),
            ),
        }
