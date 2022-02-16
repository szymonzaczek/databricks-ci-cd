import subprocess
import sys


def build_locally(setup_py: str) -> None:
    """
    Install package by calling 'python -m pip install {package}' using subprocess.
    """
    subprocess.check_call([sys.executable, setup_py, "bdist_wheel"])


def process_setup_py(
    setup_py_variable: str,
) -> None:
    """
    Function for installing dependancies during CI step.
    It processes bash variables that contains paths to requirements file and accordingly
    to the specified paremater, install given given dependancies (either common.txt or
    dev.txt).
    """
    setup_py_files = setup_py_variable.split(",")
    for file in setup_py_files:
        build_locally(file)
