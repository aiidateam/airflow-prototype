from __future__ import annotations

import os
from airflow_provider_aiida.aiida_core.manage.configuration.config import get_airflow_home


__all__ = (
    'load_profile',
)

def load_profile(profile_name: str | None = None):
    from aiida import load_profile
    profile = load_profile() if profile_name is None else load_profile(profile_name)
    airflow_home = get_airflow_home(profile) 
    os.environ.update({'AIRFLOW_HOME': str(airflow_home)})
    return profile

