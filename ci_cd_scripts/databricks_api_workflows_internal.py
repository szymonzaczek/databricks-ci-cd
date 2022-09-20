import os
import re
import time
import glob
from pathlib import Path

from read_config import read_env_cfg
from databricks_api_class_internal import DatabricksRequest

if os.environ.get("ENVIRONMENT_NAME") == "prd_bi":
    ENVIRONMENT_NAME = "prd"
else:
    ENVIRONMENT_NAME = os.environ.get("ENVIRONMENT_NAME")
print(f"Environment name: {ENVIRONMENT_NAME}")
BUILD_REPOSITORY_NAME = os.environ.get("BUILD_REPOSITORY_NAME")

# name for the folder which groups notebooks on databricks_steps workspace
local_notebooks_dirs = "notebooks"



def upload_init_script_workflow(
    cfg_path: str,
    secret_path: str,
    init_script_local_path: str,
    init_script_dbfs_path: str = "dbfs:/databricks/scripts",
):
    """
    Workflow for uploading init script to Databricks.
    By default init script is overwritten at the default path set in the function
    definition.
    """
    databricks_token = read_token_from_file(secret_path)
    cfg = read_env_cfg(ENVIRONMENT_NAME, cfg_path)
    api_object = DatabricksRequest(cfg.get("databricks_host"), None, databricks_token)
    if init_script_dbfs_path[-1] != "/":
        init_script_dbfs_path += "/"
    dbfs_path = init_script_dbfs_path + init_script_local_path.split("/")[-1]
    upload_response = api_object.upload_file_dbfs(init_script_local_path, dbfs_path)
    if upload_response == dict():
        print(f"Package has been successfully installed.")


def upload_notebooks_workflow(
    cfg_path: str,
    secret_path: str,
    notebooks_artifact_path: str,
    notebooks_target_dir: str = "/deployed/notebooks/",
):
    """
    Workflow for uploading notebooks to Databricks workspace.
    It does not need cluster info.
    """
    print(f"notebooks_target_dir: {notebooks_target_dir}")
    print(f"notebooks_artifact_path: {notebooks_artifact_path}")
    databricks_token = read_token_from_file(secret_path)
    cfg = read_env_cfg(ENVIRONMENT_NAME, cfg_path)
    api_object = DatabricksRequest(cfg.get("databricks_host"), None, databricks_token)

    if notebooks_target_dir[0] != "/":
        notebooks_target_dir = "/" + notebooks_target_dir
    if notebooks_target_dir[-1] != "/":
        notebooks_target_dir += "/"

    notebook_paths = {"local_paths": [], "db_paths": []}
    handled_extensions = ["sql", "py"]
    wildcards_supported_nesting = ["**", "**/**"]
    extension_language_mapping = {"sql": "SQL", "py": "PYTHON"}
    print(f"cwd: {os.getcwd()}")
    print(f"listdir: {os.listdir()}")

    for file_extension in handled_extensions:
        for nesting in wildcards_supported_nesting:
            print(f"path: {notebooks_artifact_path}/{nesting}/*.{file_extension}")
            lookup_path = f"{notebooks_artifact_path}/{nesting}/*.{file_extension}"
            slashes_count = lookup_path.count("/")
            slice_index = slashes_count - 2
            for entity in glob.glob(lookup_path):
                print(f"entity: {entity}")
                notebook_paths["db_paths"].append(
                    notebooks_target_dir
                    + "/".join(entity.split("/")[-slice_index:])
                )
                notebook_paths["local_paths"].append(Path(entity))

    print(f"Local paths: {notebook_paths.get('local_paths')}")
    print(f"Databricks paths: {notebook_paths.get('db_paths')}")

    if len(notebook_paths.get("local_paths")) != len(notebook_paths.get("db_paths")):
        raise ValueError(
            f"Length of local_paths is different than db_paths - THEY MUST be the same"
        )

    for x in range(len(notebook_paths.get("db_paths"))):
        print(f"current: {'/'.join(notebook_paths.get('db_paths')[x].split('/')[:-1])}")
        subdir = "/".join(notebook_paths.get("db_paths")[x].split("/")[:-1])
        notebooks_dir_status = api_object.check_if_notebook_dir_exists(subdir)
        print(f"notebooks_dir_status: {notebooks_dir_status}")
        if notebooks_dir_status.get("error_code") == "RESOURCE_DOES_NOT_EXIST":
            print("This path does not exist - proceed to creating a directory")
            response = api_object.create_directory(subdir)
            if len(response) == 0:
                print(f"Directory has been created successfully.")
            else:
                print(f"Directory was not created - response from API:\n{response}")

    print(f"Length of local paths: {(len(notebook_paths.get('local_paths')))}")
    for x in range(len(notebook_paths.get("local_paths"))):
        print(f"current db_path: {notebook_paths.get('db_paths')[x]}")
        response = api_object.upload_notebooks(
            notebook_paths.get("local_paths")[x],
            notebook_paths.get("db_paths")[x],
            extension_language_mapping.get(
                f'{str(notebook_paths.get("local_paths")[x]).split(".")[-1]}'
            ),
        )
        print(response)
        if response == dict():
            print(
                f"Notebooks have been successfully uploaded to the path:\n"
                f"{notebook_paths['python_db_paths'][x]}"
            )


def process_all_packages(
    cfg_path: str,
    secret_path: str,
    whl_files: str,
    dbfs_target_dir: str = "dbfs:/FileStore/jars/",
) -> None:
    """
    Function for processing all packages in form of a .whl file in a repository.
    This function accepts list of packages in form of a string from sys.argv
    (arguments to cli) separated by a comma.
    From this string, list of .whl files is recreated and each of those files is
    processed using process_single_package function.

    Example of whl_files:
    whl_files = "test.whl,test1.whl"
    """
    whl_files = whl_files.split(",")
    for whl_file in whl_files:
        process_single_package(
            cfg_path=cfg_path,
            secret_path=secret_path,
            whl_local_path=whl_file,
            dbfs_target_dir=dbfs_target_dir,
        )


def process_single_package(
    cfg_path: str, secret_path: str, whl_local_path: str, dbfs_target_dir: str
) -> None:
    """
    The main workflow for installing an updated wheel package on databricks cluster.
    Steps consist of:

    1. get host address and cluster id from json
    2. start cluster
    3. check if databricks_processing is installed on the cluster
    4. uninstall current version of databricks_processing
    5. restart the cluster
    6. upload whl to dbfs; overwrite if exists
    6. install wheel file

    Prints are added whenever debugging would be useful.
    In case of multiple clusters specified in the cfg file, script iterates over each
    cluster (since processing is dependant on the cluster specification.).
    """
    databricks_token = read_token_from_file(secret_path)
    print(f"Databricks token: {databricks_token}")
    cfg = read_env_cfg(ENVIRONMENT_NAME, cfg_path)
    for cluster in cfg.get("databricks_cluster_id"):
        api_object = DatabricksRequest(
            cfg.get("databricks_host"), cluster, databricks_token
        )
        current_cluster_status = api_object.check_current_cluster_status(
            api_object.get_cluster_details()
        )
        if current_cluster_status == "TERMINATED":
            api_object.start_cluster()
            time.sleep(5)
        cluster_libraries = api_object.get_cluster_libraries()
        # CAVEAT: searching for the processed package on the cluster
        installed_libraries = api_object.extract_installed_libraries_names(
            cluster_libraries
        )
        pattern = "[\W_]+"
        package_alphanumeric = re.sub(pattern, "", api_object.package)
        for installed_library in installed_libraries:
            print(f"installed library: {installed_library}")
            if "pypi" not in installed_library.keys():
                installed_library_alphanumeric = re.sub(
                    pattern, "", installed_library.get("whl")
                )
                print(
                    f"package_alphanumeric: {package_alphanumeric}\n"
                    f"installed_library_alphanumeric: {installed_library_alphanumeric}"
                )
                if package_alphanumeric in installed_library_alphanumeric:
                    print(
                        f"Specified library {installed_library} is installed on the cluster - "
                        f"uninstalling and restarting the cluster"
                    )
                    api_object.uninstall_library(installed_library)
                    api_object.restart_cluster()
                    time.sleep(5)
        while True:
            current_cluster_status = api_object.check_current_cluster_status(
                api_object.get_cluster_details()
            )
            if current_cluster_status != "RUNNING":
                wait_interval = 10
                print(
                    f"Cluster must be running for installing Python package (whl file) "
                    f"onto the cluster. Currently its status is: {current_cluster_status}."
                    f"\nNext check of the cluster status is to be done in "
                    f"{wait_interval} s."
                )
                time.sleep(wait_interval)
            else:
                break
        dbfs_path = dbfs_target_dir + whl_local_path.split("/")[-1]
        print(f"whl_local_path: {whl_local_path}\ndbfs_path: {dbfs_path}")
        print(f"Uploading and installing Python package (whl file) onto the cluster.")
        upload_output = api_object.upload_file_dbfs(whl_local_path, dbfs_path)
        print(f"Upload output: {upload_output}; file was uploaded")
        installation_output = api_object.install_whl(dbfs_path)
        if installation_output == dict():
            print(f"Package has been successfully installed.")
        else:
            print(f"installation output: {installation_output}")


def read_token_from_file(file: str) -> str:
    """
    Read databricks_token from a file (provided in secrets.txt).
    """
    with open(file, "r") as f:
        databricks_token = f.read().replace("\n", "")
    return databricks_token


def process_dependencies(cfg_path: str, secret_path: str, requirements_variable: str):
    """
    Install dependencies found in the repository on the clusters specified in
    config.json.
    """
    databricks_token = read_token_from_file(secret_path)
    cfg = read_env_cfg(ENVIRONMENT_NAME, cfg_path)
    requirements_files = requirements_variable.split(",")
    for cluster in cfg.get("databricks_cluster_id"):
        api_object = DatabricksRequest(
            cfg.get("databricks_host"), cluster, databricks_token
        )
        current_cluster_status = api_object.check_current_cluster_status(
            api_object.get_cluster_details()
        )
        if current_cluster_status == "TERMINATED":
            api_object.start_cluster()
            time.sleep(5)
        libraries_to_install = []
        for file in requirements_files:
            with open(file, "r") as f:
                for line in f.readlines():
                    library_to_install = "".join(line.split())
                    libraries_to_install.append(library_to_install)
        for library_to_install in libraries_to_install:
            print(f"Library to be installed on the cluster: {library_to_install}")
            response = api_object.install_library_pip(library_to_install)
            print(f"response: {response}")
