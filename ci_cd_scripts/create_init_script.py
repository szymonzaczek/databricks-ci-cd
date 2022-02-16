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


def create_init_script(content: str, file_path: str) -> None:
    """
    Function for writing init script content to a file.
    This file will be uploaded to databricks as init script.
    """
    with open(file_path, "w") as f:
        f.write(content)
