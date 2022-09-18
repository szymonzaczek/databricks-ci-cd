import glob
from shutil import copy
from pathlib import Path


def discover_and_copy_notebooks_workflow(
    working_dir: str, subdir: str, target_dir: str, pattern: str = r".notebooks"
):
    """
    Notebook for discovering and copying notebooks from the repo
    into an artifact.

    :param working_dir: working directory for Python interpreter
    :type working_dir: str
    :type subdir: nested subdirectories
    :type subdir: str
    :param
    """
    notebooks_local_paths = []
    notebooks_target_paths = []
    notebooks_path_patterns = [
        "*.py",
        "*/*.py",
        "*.sql",
        "*/*.sql",
    ]  # includes one level of subdirectories

    print(f"Working dir: {working_dir}\nsubdir: {subdir}\ntarget dir: {target_dir}")
    for path_pattern in notebooks_path_patterns:
        print(f"path: {str(working_dir)}/*/{subdir}/{path_pattern}")
        for entity in glob.glob(
            f"{str(working_dir)}/*/{subdir}/{path_pattern}"
        ):
            print(f"notebook files paths: {entity}")
            notebooks_local_paths.append(Path(entity))

    pattern = r".notebooks"

    for x in range(len(notebooks_local_paths)):
        domain_name = str(notebooks_local_paths[x]).split("/")[6]
        target_sub_path_relative = (
            "/"
            + domain_name
            + "/"
            + "/".join(str(notebooks_local_paths[x]).split("/")[8:])
        )
        print(f"target_sub_path_relative: {target_sub_path_relative}")
        final_path = Path(
            target_dir.__str__() + "/" + target_sub_path_relative.__str__()
        )
        notebooks_target_paths.append(final_path)

    for i, file in enumerate(notebooks_local_paths):
        dir_path = Path(
            "/".join(
                notebooks_target_paths[i].__str__().replace("\\", "/").split("/")[:-1]
            )
        )
        print(f"File: {file}; dir_path: {dir_path}")
        dir_path.mkdir(parents=True, exist_ok=True)
        copy(file, notebooks_target_paths[i])
