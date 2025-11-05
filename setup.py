"""Setup script to generate DAG files during installation."""
from setuptools import setup
from setuptools.command.install import install
from setuptools.command.develop import develop
import subprocess
import sys


class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        print("\n" + "="*60)
        print("Generating AiiDA DAG files...")
        print("="*60)
        subprocess.check_call([sys.executable, "scripts/generate_dags.py"])
        print("="*60 + "\n")


class PostDevelopCommand(develop):
    """Post-installation for development mode."""
    def run(self):
        develop.run(self)
        print("\n" + "="*60)
        print("Generating AiiDA DAG files...")
        print("="*60)
        subprocess.check_call([sys.executable, "scripts/generate_dags.py"])
        print("="*60 + "\n")


setup(
    cmdclass={
        'install': PostInstallCommand,
        'develop': PostDevelopCommand,
    },
)
