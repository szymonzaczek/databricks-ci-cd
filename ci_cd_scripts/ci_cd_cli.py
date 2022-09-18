import sys
from build_packages import process_setup_py
from copy_files import copy_requirements, copy_files
from read_config import read_env_cfg, read_flat_cfg
from discover_and_copy_notebooks import discover_and_copy_notebooks_workflow
from find_files import find_files_job, find_files_in_nested_dir_job
from process_requirements_locally import process_requirements
from create_init_script import create_init_script_workflow
from databricks_api_workflows_internal import (
    upload_notebooks_workflow,
    process_all_packages,
    upload_init_script_workflow,
    process_dependencies
)


cli_args = sys.argv[1:]
allowed_first_cli_args = [
    "copy_files", "discover_and_copy_notebooks_workflow", "read_env_cfg", "read_flat_cfg",
    "upload_notebooks_workflow", "find_files_job", "find_files_in_nested_dir_job",
    "process_all_packages", "process_requirements", "process_setup_py",
    "upload_init_script_workflow", "create_init_script_workflow",
    "process_dependencies", "copy_requirements"
]

if cli_args[0] not in allowed_first_cli_args:
    raise ValueError("Invalid CLI command. Use one of %s" % allowed_first_cli_args)

if len(cli_args) == 1:
    evaluation_string = f"{cli_args[0]}()"
    print(f"evaluation_string: {evaluation_string}")
    eval(evaluation_string)
elif len(cli_args) == 2:
    evaluation_string = f"{cli_args[0]}('{cli_args[1]}')"
    print(f"evaluation_string: {evaluation_string}")
    eval(evaluation_string)
elif len(cli_args) > 2:
    evaluation_string = f"{cli_args[0]}{*cli_args[1:],}"
    print(f"evaluation_string: {evaluation_string}")
    eval(evaluation_string)
