from __future__ import annotations

from airflow_provider_aiida.aiida_core.engine.daemon._supervisor import (
        ServiceSupervisorController,
        NonWorkerServiceConfig,
        ServiceConfigFactory,
        ServiceConfigMap
    )
from pathlib import Path
from dataclasses import dataclass
from typing import ClassVar, TYPE_CHECKING
import logging

from airflow_provider_aiida.aiida_core.manage.configuration.config import get_airflow_home

if TYPE_CHECKING:
    from aiida.manage.configuration import Profile
    from aiida.manage.configuration.config import Config 

logger = logging.getLogger(__name__)

# TODO move to aiida something
def get_daemon_dir(profile: Profile, config: Config):
    from aiida.manage.configuration.settings import AiiDAConfigPathResolver
    config_path_resolver: AiiDAConfigPathResolver = AiiDAConfigPathResolver(Path(config.dirpath))
    daemon_dir = config_path_resolver.daemon_dir
    return daemon_dir / f"{profile.name}"

@dataclass
class AirflowDagProcessorServiceConfig(NonWorkerServiceConfig):
    service_name: ClassVar[str] = "airflow-dag-processor"
    command: ClassVar[str] = "airflow dag-processor"
    airflow_home: str

    def _new_env(self) -> dict[str, str]:
        return {'AIRFLOW_HOME': self.airflow_home}

@dataclass
class AirflowSchedulerServiceConfig(NonWorkerServiceConfig):
    # We do not want any limit on this
    service_name: ClassVar[str] = "airflow-scheduler"
    command: ClassVar[str] = "airflow scheduler"
    airflow_home: str 
    num_workers: int


    def _new_env(self) -> dict[str, str]:
        return {'AIRFLOW_HOME': self.airflow_home,
                'AIRFLOW__CORE__PARALLELISM': str(self.num_workers)}

@dataclass
class AirflowTriggererServiceConfig(NonWorkerServiceConfig):
    # We do not want any limit on this
    service_name: ClassVar[str] = "airflow-triggerer"
    command: ClassVar[str] = "airflow-provider-aiida-triggerer-service"
    airflow_home: str
    num_triggerers: int


    def _new_env(self) -> dict[str, str]:
        return {
            'AIRFLOW_HOME': self.airflow_home,
            'AIRFLOW__CORE__ASYNC_PARALLELISM': str(self.num_triggerers)}


# TODO this is not used but the idea is to make the env types generic
from typing import TypedDict
class AirflowApiServerEnv(TypedDict):
    AIRFLOW_HOME: str
    AIRFLOW__API__HOST: str 
    AIRFLOW__API__PORT: str 


@dataclass
class AirflowApiServerServiceConfig(NonWorkerServiceConfig):
    # We do not want any limit on this
    service_name: ClassVar[str] = "airlfow-api-server"
    command: ClassVar[str] = "airflow api-server"
    airflow_home: str

    def _new_env(self) -> dict[str, str]:
        from airflow.configuration import AirflowConfigParser
        airflow_config = AirflowConfigParser()
        airflow_config_file = Path(self.airflow_home) / 'airflow.cfg'
        airflow_config.read(airflow_config_file)

        return {
            'AIRFLOW_HOME': self.airflow_home,
        }

class AirflowDaemon:

    def __init__(self, profile_identifier):
        from aiida.manage import get_manager 
        manager = get_manager()
        profile = manager.load_profile() if profile_identifier is None else manager.load_profile(profile_identifier)

        # Validate profile storage backend
        if profile.storage_backend != 'core.psql_dos':
            raise ValueError(
                f"Profile '{profile.name}' uses unsupported storage backend '{profile.storage_backend}'. "
                f"Only 'core.psql_dos' (PostgreSQL) is supported."
        )
        self._daemon_dir = get_daemon_dir(profile, manager.get_config())
        self._daemon_dir.mkdir(exist_ok=True)

        self._airflow_home = get_airflow_home(profile)

    def start(self, num_workers: int, num_triggerers: int, foreground: bool):
        scheduler_config = AirflowSchedulerServiceConfig(num_workers=num_workers, airflow_home=str(self._airflow_home))
        dag_processor_config = AirflowDagProcessorServiceConfig(airflow_home=str(self._airflow_home)) 
        api_server_config = AirflowApiServerServiceConfig(airflow_home=str(self._airflow_home))
        triggerer_config = AirflowTriggererServiceConfig(num_triggerers=num_triggerers, airflow_home=str(self._airflow_home))

        service_configs = ServiceConfigMap([scheduler_config, dag_processor_config, api_server_config, triggerer_config])
        ServiceSupervisorController.start(self._daemon_dir, service_configs, foreground)

    def stop(self):
        ServiceSupervisorController.stop(self._daemon_dir)

    def status(self) -> dict:
        status_report = ServiceSupervisorController.status(self._daemon_dir)
        if (configs := ServiceSupervisorController.get_service_configs(self._daemon_dir)) is not None:
            for config in configs.values():
                if isinstance(config, AirflowSchedulerServiceConfig):
                    status_report['services'][f'{config.service_name}']['num_workers'] = config.num_workers
                elif isinstance(config, AirflowTriggererServiceConfig):
                    status_report['services'][f'{config.service_name}']['num_workers'] = config.num_triggerers

        return status_report
