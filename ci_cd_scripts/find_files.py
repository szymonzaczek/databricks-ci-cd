import glob


def find_files_in_a_path_with_extension(path: str, pattern: str) -> list:
    """
    Find .{pattern} files in the provided path.
    """
    files_local_paths = []
    if path[-1] != "/":
        path += "/"
    search_path = path + pattern
    for file in glob.glob(search_path):
        files_local_paths.append(file)
    return files_local_paths


def output_list_as_bash_variable_ado(list_to_output: list, variable_name: str) -> None:
    """
    Converting list to bash variable on Azure DevOps.
    This bash variable is a string representation of a given list, where arguments are
    separated by a comma.
    """
    string_output = ",".join(list_to_output)
    print(f"##vso[task.setvariable variable={variable_name}]{string_output}")


def find_files_job(
    working_dir: str,
    pattern: str,
    variable_name_prefix: str = None,
    variable_name_suffix: str = "_files",
) -> None:
    """
    Workflow for finding files in a given working directory.
    """
    if variable_name_prefix is None:
        variable_name = pattern.split(".")[-1] + variable_name_suffix
    else:
        variable_name = variable_name_prefix + variable_name_suffix
    files_local_paths = find_files_in_a_path_with_extension(working_dir, pattern)
    output_list_as_bash_variable_ado(files_local_paths, variable_name)


def find_files_in_nested_dir(
    path: str,
    nested_dir_depth: str,
    nested_dir: str,
    extension: str = "*",
    filename: str = "*",
) -> list:
    """
    Find files in a specific nested directory inside of the working directory.
    Level of nestedness can be controll via nested_dir_depth.
    It can also take "**" as argument to nested_dir - it will mean that it will search
    all directories present in a given path.
    working_dir should be absolute path.
    """
    nested_dir_depth = int(nested_dir_depth)
    if path[-1] != "/":
        path += "/"
    search_path = path + nested_dir_depth * f"**/"
    search_path += f"{nested_dir}/{filename}.{extension}"
    files_list = []
    for entity in glob.glob(search_path, recursive=True):
        files_list.append(entity)
    print(f"files_list: {files_list}")
    return files_list


def find_files_in_nested_dir_job(
    working_dir: str,
    nested_dir_depth: str,
    nested_dir: str,
    extension: str = "*",
    filename: str = "*",
    variable_name_suffix: str = "_files",
) -> None:
    """
    Workflow for finding files in nested directories.
    """
    if filename != "*":
        variable_name = filename + variable_name_suffix
    else:
        variable_name = nested_dir + variable_name_suffix
    if nested_dir == "None":
        nested_dir = "**"
    files_local_paths = find_files_in_nested_dir(
        working_dir, nested_dir_depth, nested_dir, extension, filename
    )
    output_list_as_bash_variable_ado(files_local_paths, variable_name)
