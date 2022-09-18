from shutil import copy


def copy_files(artifact_dir: str, files_variable: str) -> None:
    """
    Copy all of the files provided in files_variable to the specified directory.
    """
    files = files_variable.split(",")
    for file in files:
        target_path = artifact_dir + file.split("/")[-1]
        copy(file, target_path)
        # TODO: REMOVE - it's just for debugging purposes
        print(f"file: {file}\ntarget: {target_path}")
        with open(file, 'rb') as f:
            print(f"Target path content:\n{f.read()}")
        with open(target_path, 'rb') as f:
            print(f"Target path content:\n{f.read()}")




def copy_requirements(
    artifact_dir: str, requirements_variable: str, requirements_type: str = "common"
) -> None:
    """
    Collect all requirement files with a specified requirement type.
    Write a new file (requirements.txt) containing all of the requirements from the
    collected files in artifact_dir.
    """
    requirements = requirements_variable.split(",")
    requirements_files = []
    for req_file in requirements:
        if req_file.split("/")[-1].split(".")[-2] == requirements_type:
            requirements_files.append(req_file)
    if artifact_dir[-1] != "/":
        artifact_dir += "/"
    artifact_requirement_file_content = ""
    for req_file in requirements_files:
        with open(req_file, "r") as f:
            artifact_requirement_file_content += f.read()
    with open(f"{artifact_dir}requirements.txt", "w") as file_to_write:
        file_to_write.write(artifact_requirement_file_content)
