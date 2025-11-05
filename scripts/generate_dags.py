#!/usr/bin/env python3
"""
Generate physical DAG files for all registered AiiDA plugins.
Run once during installation to create DAG files.
"""
import os
from pathlib import Path
from importlib_metadata import entry_points


def generate_calcjob_dag_file(entry_point_name: str, class_path: str, output_dir: Path):
    """Generate a DAG file for a CalcJob."""
    dag_id = f"aiida_calcjob_{entry_point_name.replace('.', '_')}"
    filename = f"{dag_id}.py"

    content = f'''"""Auto-generated DAG for AiiDA CalcJob: {entry_point_name}"""
from airflow import DAG
from airflow.models.param import Param
from airflow_provider_aiida.taskgroups.calcjob import CalcJobTaskGroup
from {class_path.rsplit('.', 1)[0]} import {class_path.rsplit('.', 1)[1]}

with DAG(
    dag_id='{dag_id}',
    description='Auto-generated DAG for AiiDA CalcJob: {entry_point_name}',
    params={{
        "node_pk": Param("", type="integer", description="AiiDA node PK to resume execution")
    }},
    render_template_as_native_obj=True,
    tags=['aiida', 'auto-generated', 'calcjob', '{entry_point_name}'],
    catchup=False,
) as dag:
    task = CalcJobTaskGroup(
        process_class={class_path.rsplit('.', 1)[1]},
        node_pk="{{{{ params.node_pk }}}}",
    )
'''

    filepath = output_dir / filename
    filepath.write_text(content)
    return filename


def generate_workchain_dag_file(entry_point_name: str, class_path: str, output_dir: Path):
    """Generate a DAG file for a WorkChain."""
    dag_id = f"aiida_workchain_{entry_point_name.replace('.', '_')}"
    filename = f"{dag_id}.py"

    content = f'''"""Auto-generated DAG for AiiDA WorkChain: {entry_point_name}"""
from airflow import DAG
from airflow.models.param import Param
from airflow_provider_aiida.taskgroups.workchain import WorkChainTaskGroup
from {class_path.rsplit('.', 1)[0]} import {class_path.rsplit('.', 1)[1]}

with DAG(
    dag_id='{dag_id}',
    description='Auto-generated DAG for AiiDA WorkChain: {entry_point_name}',
    params={{
        "node_pk": Param("", type="integer", description="AiiDA node PK to resume execution")
    }},
    render_template_as_native_obj=True,
    tags=['aiida', 'auto-generated', 'workchain', '{entry_point_name}'],
    catchup=False,
) as dag:
    task = WorkChainTaskGroup(
        process_class={class_path.rsplit('.', 1)[1]},
        node_pk="{{{{ params.node_pk }}}}",
    )
'''

    filepath = output_dir / filename
    filepath.write_text(content)
    return filename


def main():
    """Generate all DAG files."""
    # Output directory
    package_root = Path(__file__).parent.parent
    output_dir = package_root / "src" / "airflow_provider_aiida" / "auto_generated_dags"
    output_dir.mkdir(exist_ok=True)

    print(f"Generating DAG files in: {output_dir}")

    generated_files = []

    # Discover and generate CalcJob DAGs
    eps = entry_points()
    calc_eps = eps.select(group='aiida.calculations')

    for ep in calc_eps:
        try:
            plugin_class = ep.load()
            # Skip if not a class (some are functions)
            if not isinstance(plugin_class, type):
                continue

            class_path = f"{plugin_class.__module__}.{plugin_class.__name__}"
            filename = generate_calcjob_dag_file(ep.name, class_path, output_dir)
            generated_files.append(filename)
            print(f"  ✓ Generated: {filename}")
        except Exception as e:
            print(f"  ✗ Failed to generate DAG for {ep.name}: {e}")

    # Discover and generate WorkChain DAGs
    workflow_eps = eps.select(group='aiida.workflows')

    for ep in workflow_eps:
        try:
            plugin_class = ep.load()
            # Skip if not a class (some are functions)
            if not isinstance(plugin_class, type):
                continue

            class_path = f"{plugin_class.__module__}.{plugin_class.__name__}"
            filename = generate_workchain_dag_file(ep.name, class_path, output_dir)
            generated_files.append(filename)
            print(f"  ✓ Generated: {filename}")
        except Exception as e:
            print(f"  ✗ Failed to generate DAG for {ep.name}: {e}")

    # Generate __init__.py to import all DAGs
    init_content = '"""Auto-generated DAGs for AiiDA plugins."""\n\n'
    init_content += "# Import all generated DAGs\n"
    for filename in sorted(generated_files):
        module_name = filename.replace('.py', '')
        init_content += f"from .{module_name} import dag as {module_name}\n"

    init_content += "\n# Export all DAGs\n__all__ = [\n"
    for filename in sorted(generated_files):
        module_name = filename.replace('.py', '')
        init_content += f"    '{module_name}',\n"
    init_content += "]\n"

    (output_dir / "__init__.py").write_text(init_content)
    print(f"\n✅ Generated {len(generated_files)} DAG files + __init__.py")
    return len(generated_files)


if __name__ == "__main__":
    import sys
    count = main()
    sys.exit(0 if count > 0 else 1)
