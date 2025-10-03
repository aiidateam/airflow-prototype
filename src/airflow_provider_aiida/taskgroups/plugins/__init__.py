"""Airflow-compatible CalcJob implementations."""

from airflow_provider_aiida.taskgroups.plugins.pw import PwCalculation

__all__ = ['PwCalculation']
