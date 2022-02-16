import subprocess
import sys


def install_locally(package: str) -> None:
    """
    Install package by calling 'python -m pip install {package}' using subprocess.
    """
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", package])


def process_requirements(
    requirements_variable: str,
    requirements_type: str,
) -> None:
    """
    Function for installing dependancies during CI step.
    It processes bash variables that contains paths to requirements file and accordingly
    to the specified paremater, install given given dependancies (either common.txt or
    dev.txt).
    """
    requirements = requirements_variable.split(",")
    requirements_files = []
    for req_file in requirements:
        if req_file.split("/")[-1].split(".")[-2] == requirements_type:
            requirements_files.append(req_file)
    for file in requirements_files:
        install_locally(file)