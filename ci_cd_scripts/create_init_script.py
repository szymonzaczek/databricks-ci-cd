import os


def build_script_content(
    package_dbfs_dir: str, requirements_files: tuple, *args
) -> str:
    """
    Create init script content for Databricks.
    This script is used for initiating clusters.
    It should contain pip install statements for all the packages created in the
    following release process.

    *args - whl files
    """
    package_dbfs_dir = package_dbfs_dir.replace(":", "")
    init_script_content = f"#!/bin/bash\n" f"pip install --upgrade pip\n"
    if requirements_files is not None:
        for file in requirements_files:
            with open(file, "r") as f:
                for line in f.readlines():
                    init_script_content += f"pip install {line}"
            init_script_content += "\n"
    for arg in args:
        arg = arg.split("/")[-1]
        init_script_content += f"pip install {package_dbfs_dir}/{arg}\n"
    return init_script_content


def write_init_script_to_file(content: str, file_path: str) -> None:
    """
    Function for writing init script content to a file.
    This file will be uploaded to databricks_steps as init script.
    """
    with open(file_path, "w") as f:
        f.write(content)


def create_init_script_workflow(
    working_dir: str,
    init_script_local_path: str,
    package_dbfs_dir: str,
    whl_files_variable: str,
    requirements_variable: str,
):
    """
    Workflow for creating init scripts

    :param working_dir: working directory,
    :type working_dir: str
    :param init_script_local_path: local path for writing init script,
    :type init_script_local_path: str
    :param package_dbfs_dir: package_dbfs_dir path,
    :type package_dbfs_dir: str
    :param whl_files_variable: env variable with wheel files,
    :type whl_files_variable: str
    :param requirements_variable: env variable with requirements,
    :type requirements_variable: str
    """
    os.chdir(str(working_dir))
    whl_files = whl_files_variable.split(",")
    requirements = requirements_variable.split(",")

    requirements_files = []
    for req_file in requirements:
        # init script is always for worklads, so it will never use dev.txt requirements
        if req_file.split("/")[-1] == "common.txt":
            requirements_files.append(req_file)

    requirements_files = tuple(requirements_files)

    init_script = build_script_content(
        str(package_dbfs_dir), requirements_files, *whl_files
    )
    write_init_script_to_file(init_script, str(init_script_local_path))
