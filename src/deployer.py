import re
from typing import Dict

from lifecycle.auth.subject import get_auth_subject_by_job_family
from lifecycle.config import Config
from lifecycle.deployer.base import JobDeployer
from lifecycle.deployer.secrets import JobSecrets
from lifecycle.job.models_registry import read_job_family_model
from racetrack_client.client.env import merge_env_vars
from racetrack_client.client.run import JOB_INTERNAL_PORT
from racetrack_client.log.logs import get_logger
from racetrack_client.manifest import Manifest
from racetrack_client.utils.shell import shell, shell_output
from racetrack_client.utils.time import datetime_to_timestamp, now
from racetrack_commons.api.tracing import get_tracing_header_name
from racetrack_commons.deploy.image import get_job_image
from racetrack_commons.deploy.resource import job_resource_name
from racetrack_commons.entities.dto import JobDto, JobStatus, JobFamilyDto
from racetrack_commons.plugin.core import PluginCore
from racetrack_commons.plugin.engine import PluginEngine

logger = get_logger(__name__)


class DockerJobDeployer(JobDeployer):
    """JobDeployer managing workloads on a local docker instance, used mostly for testing purposes"""

    def __init__(self, secrets_store: dict[str, JobSecrets]) -> None:
        self.infrastructure_name = 'docker'
        self._secrets_store: dict[str, JobSecrets] = secrets_store

    def deploy_job(
        self,
        manifest: Manifest,
        config: Config,
        plugin_engine: PluginEngine,
        tag: str,
        runtime_env_vars: Dict[str, str],
        family: JobFamilyDto,
        containers_num: int = 1,
    ) -> JobDto:
        """Run Job as docker container on local docker"""
        if self.job_exists(manifest.name, manifest.version):
            self.delete_job(manifest.name, manifest.version)

        entrypoint_resource_name = job_resource_name(manifest.name, manifest.version)
        deployment_timestamp = datetime_to_timestamp(now())
        family_model = read_job_family_model(family.name)
        auth_subject = get_auth_subject_by_job_family(family_model)

        common_env_vars = {
            'PUB_URL': config.internal_pub_url,
            'JOB_NAME': manifest.name,
            'AUTH_TOKEN': auth_subject.token,
            'JOB_DEPLOYMENT_TIMESTAMP': deployment_timestamp,
            'REQUEST_TRACING_HEADER': get_tracing_header_name(),
        }
        if config.open_telemetry_enabled:
            common_env_vars['OPENTELEMETRY_ENDPOINT'] = config.open_telemetry_endpoint

        if containers_num > 1:
            common_env_vars['JOB_USER_MODULE_HOSTNAME'] = self.get_container_name(entrypoint_resource_name, 1)

        conflicts = common_env_vars.keys() & runtime_env_vars.keys()
        if conflicts:
            raise RuntimeError(f'found illegal runtime env vars, which conflict with reserved names: {conflicts}')
        runtime_env_vars = merge_env_vars(runtime_env_vars, common_env_vars)
        plugin_vars_list = plugin_engine.invoke_plugin_hook(PluginCore.job_runtime_env_vars)
        for plugin_vars in plugin_vars_list:
            if plugin_vars:
                runtime_env_vars = merge_env_vars(runtime_env_vars, plugin_vars)
        env_vars_cmd = ' '.join([f'--env {env_name}="{env_val}"' for env_name, env_val in runtime_env_vars.items()])

        for container_index in range(containers_num):

            container_name = self.get_container_name(entrypoint_resource_name, container_index)
            image_name = get_job_image(config.docker_registry, config.docker_registry_namespace, manifest.name, tag, container_index)

            shell(
                f'docker run -d'
                f' --name {container_name}'
                f' {env_vars_cmd}'
                f' --pull always'
                f' --network="racetrack_default"'
                f' --add-host=host.docker.internal:host-gateway'
                f' --label job-name={manifest.name}'
                f' --label job-version={manifest.version}'
                f' {image_name}'
            )

        return JobDto(
            name=manifest.name,
            version=manifest.version,
            status=JobStatus.RUNNING.value,
            create_time=deployment_timestamp,
            update_time=deployment_timestamp,
            manifest=manifest,
            internal_name=f'{entrypoint_resource_name}:{JOB_INTERNAL_PORT}',
            image_tag=tag,
            infrastructure_target=self.infrastructure_name,
        )

    def delete_job(self, job_name: str, job_version: str):
        entrypoint_resource_name = job_resource_name(job_name, job_version)
        for container_index in range(2):
            container_name = self.get_container_name(entrypoint_resource_name, container_index)
            self._delete_container_if_exists(container_name)

    def job_exists(self, job_name: str, job_version: str) -> bool:
        resource_name = job_resource_name(job_name, job_version)
        container_name = self.get_container_name(resource_name, 0)
        return self._container_exists(container_name)

    @staticmethod
    def _container_exists(container_name: str) -> bool:
        output = shell_output(f'docker ps -a --filter "name=^/{container_name}$" --format "{{{{.Names}}}}"')
        return container_name in output.splitlines()

    def _delete_container_if_exists(self, container_name: str):
        if self._container_exists(container_name):
            shell(f'docker rm -f {container_name}')

    @staticmethod
    def _get_next_job_port() -> int:
        """Return next unoccupied port for Job"""
        output = shell_output('docker ps --filter "name=^/job-" --format "{{.Names}} {{.Ports}}"')
        occupied_ports = set()
        for line in output.splitlines():
            match = re.fullmatch(r'job-(.+) .+:(\d+)->.*', line.strip())
            if match:
                occupied_ports.add(int(match.group(2)))
        for port in range(7000, 8000, 10):
            if port not in occupied_ports:
                return port
        return 8000

    def save_job_secrets(self,
                         job_name: str,
                         job_version: str,
                         job_secrets: JobSecrets,
                         ):
        logger.warning('saving secrets in an ephemeral, in-memory store')
        self._secrets_store[f'{job_name}.{job_version}'] = job_secrets

    def get_job_secrets(self,
                        job_name: str,
                        job_version: str,
                        ) -> JobSecrets:
        key = f'{job_name}.{job_version}'
        if key in self._secrets_store:
            return self._secrets_store[key]
        raise NotImplementedError("managing secrets is not supported on local docker")

    @staticmethod
    def get_container_name(resource_name: str, container_index: int) -> str:
        if container_index == 0:
            return resource_name
        else:
            return f'{resource_name}-{container_index}'
