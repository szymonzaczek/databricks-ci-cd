import requests
import os
import sys
import json
import re
import time
import glob
import base64
import datetime
from pathlib import Path
from typing import Union

from read_config import read_cfg


ENVIRONMENT_NAME = os.environ.get("ENVIRONMENT_NAME")
print(f"Environment name: {ENVIRONMENT_NAME}")
BUILD_REPOSITORY_NAME = os.environ.get("BUILD_REPOSITORY_NAME")

# name for the folder which groups notebooks on databricks workspace
local_notebooks_dirs = "notebooks"


class DatabricksRequest:
    """
    Class for the communication between Azure Devops and Databricks API.
    It uses config from the json file as well as some environment variables (such as
    databricks_token).
    This is a handler class - it contains some generalized ways for communicating with
    Databricks API.
    Processing of packages is separated into a separate function (process_package),
    that is outside the scope of this class.
    It mainly focuses on interaction with clusters API.
    """

    def __init__(
        self, host: str, cluster_id: Union[str, None], databricks_token: str
    ) -> None:
        self.host = host
        if self.host[-1] != "/":
            self.host = self.host + "/"
        self.url = self.host + "api/2.0/"
        self.headers = {"Authorization": f"Bearer {databricks_token}"}
        self.payload = {"cluster_id": cluster_id}
        self.package = BUILD_REPOSITORY_NAME

    def check_current_cluster_status(self, cluster_details):
        """
        Check the status of the cluster. Available statuses:
        ["PENDING", "RUNNING, "RESTARTING, "RESIZING", "TERMINATING", "TERMINATED"]

        Argument - output of self.get_cluster_details()
        """
        return cluster_details.get("state")

    def get_cluster_details(self) -> dict:
        """
        Check the cluster details.
        """
        url = self.url + "clusters/get"
        response = requests.get(url, headers=self.headers, json=self.payload)
        if response.status_code != 200:
            print(response.text)
            return response.text
        else:
            return json.loads(response.text)

    def start_cluster(self) -> str:
        """
        Start the cluster.
        """
        url = self.url + "clusters/start"
        response = requests.post(url, headers=self.headers, json=self.payload)
        return response.text

    def get_cluster_libraries(self) -> dict:
        """
        Returns details about installed libraries on the cluster.
        """
        url = self.url + "libraries/cluster-status"
        response = requests.get(url, headers=self.headers, json=self.payload)
        return json.loads(response.text)

    def extract_installed_libraries_names(self, cluster_libraries: dict) -> list:
        """
        Extract libraries names from self.get_cluster_libraries().
        """
        libraries = cluster_libraries.get("library_statuses")
        libraries_list = []
        if libraries is not None:
            for library in libraries:
                libraries_list.append(library.get("library"))
        return libraries_list

    def uninstall_library(self, library: dict) -> str:
        """
        Uninstall a library from the cluster.

        Example usage:
        api_object = DatabricksRequest(cfg)
        library = {'whl': 'dbfs:/FileStore/jars/databricks_processing-0.0.1-py3-none-any.whl'}
        api_object.uninstall_library(library)
        """
        url = self.url + "libraries/uninstall"
        payload = self.payload
        payload["libraries"] = [library]
        response = requests.post(url, headers=self.headers, json=payload)
        return response.text

    def restart_cluster(self) -> str:
        """
        Restart the cluster
        """
        url = self.url + "clusters/restart"
        response = requests.post(url, headers=self.headers, json=self.payload)
        return response.text

    def upload_file_dbfs(self, file_local_path: str, dbfs_path: str) -> str:
        """
        Upload file to DBFS.
        """
        url = self.url + "dbfs/put"
        with open(file_local_path, "rb") as whl_file:
            payload = {"path": dbfs_path, "overwrite": True}
            files = {"file": whl_file}
            response = requests.post(
                url, headers=self.headers, data=payload, files=files
            )
        return response.text

    def install_whl(self, dbfs_path: str) -> str:
        """
        Install whl file from DBFS on a given cluster.
        """
        url = self.url + "libraries/install"
        payload = self.payload
        payload["libraries"] = {"whl": dbfs_path}
        response = requests.post(url, headers=self.headers, json=self.payload)
        print(response)
        return response.text

    def install_library_pip(self, library: str) -> str:
        """
        Install a library from PYPI repository - equals to `pip install <library>`.
        """
        url = self.url + "libraries/install"
        payload = self.payload
        payload["libraries"] = {"pypi": {"package": f"{library}"}}
        response = requests.post(url, headers=self.headers, json=self.payload)
        return response.text

    def check_if_notebook_dir_exists(self, notebooks_dir: str) -> dict:
        """
        Check if the parent directory for notebooks exists.
        """
        url = self.url + "workspace/get-status"
        payload = {"path": f"{notebooks_dir}"}
        print(
            f"Check if notebook dir exists \nurl: {url}\npayload: {payload}\n"
            f"headers: {self.headers}"
        )
        response = requests.get(url, headers=self.headers, json=payload)
        return json.loads(response.text)

    def create_directory(self, notebooks_dir: str) -> dict:
        """
        Create directory in the workspace.
        """
        url = self.url + "workspace/mkdirs"
        payload = {"path": f"{notebooks_dir}"}
        response = requests.post(url, headers=self.headers, json=payload)
        return json.loads(response.text)

    def upload_notebooks(
        self, local_notebook_path: str, target_path: str, language: str = "PYTHON"
    ):
        """
        Upload notebooks from the artifact to the given path on the workspace.
        Target path is stripped of notebook subdirectory.
        """
        url = self.url + "workspace/import"
        headers = self.headers
        with open(f"{local_notebook_path}", "rb") as notebook_file:
            notebook_file = notebook_file.read()
            encoded = base64.b64encode(notebook_file)
            decoded_utf8 = encoded.decode("UTF-8")
            payload = {
                "path": f"{target_path}",
                "format": "SOURCE",
                "language": f"{language}",
                "overwrite": "true",
                "content": decoded_utf8,
            }
            response = requests.post(url, headers=headers, json=payload)
            return response.text


def read_token_from_file(file: str) -> str:
    """
    Read databricks_token from a file (provided in secrets.txt).
    """
    with open(file, "r") as f:
        databricks_token = f.read().replace("\n", "")
    return databricks_token


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
    Steps consists of:

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
    cfg = read_cfg(ENVIRONMENT_NAME, cfg_path)
    # CAVEAT: in case of multiple clusters specified in the cfg file, script iterates
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
                        f"Specified library {installed_library} is installed on the "
                        f"cluster - uninstalling and restarting the cluster"
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
                    f"onto the cluster. Currently its status is: "
                    f"{current_cluster_status}.\nNext check of the cluster status is to"
                    f" be done in {wait_interval} s."
                )
                time.sleep(wait_interval)
            else:
                break
        dbfs_path = dbfs_target_dir + whl_local_path.split("/")[-1]
        print(f"whl_local_path: {whl_local_path}\ndbfs_path: {dbfs_path}")
        print(f"Uploading and installing Python package (whl file) onto the cluster.")
        print(f"whl_local_path: {whl_local_path}\ndbfs_path: {dbfs_path}")
        upload_output = api_object.upload_file_dbfs(whl_local_path, dbfs_path)
        print(f"Upload output: {upload_output}; file was uploaded")
        installation_output = api_object.install_whl(dbfs_path)
        if installation_output == dict():
            print(f"Package has been successfully installed.")
        else:
            print(f"installation output: {installation_output}")


def process_dependancies(cfg_path: str, secret_path: str, requirements_variable: str):
    """
    Install dependencies found in the repository on the clusters specified in
    config.json.
    """
    databricks_token = read_token_from_file(secret_path)
    cfg = read_cfg(ENVIRONMENT_NAME, cfg_path)
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
        cluster_libraries = api_object.get_cluster_libraries()
        installed_libraries = api_object.extract_installed_libraries_names(
            cluster_libraries
        )
        restart_flag = False
        for installed_library_raw in installed_libraries:
            print(f"installed library: {installed_library_raw}")
            if "pypi" in installed_library_raw.keys():
                installed_library = installed_library_raw.get("pypi").get("package")
                print(f"installed library: {installed_library_raw}")
                if installed_library in libraries_to_install:
                    print(
                        f"Specified library {installed_library_raw} is installed on the"
                        f" cluster - uninstalling and restarting the cluster"
                    )
                    response = api_object.uninstall_library(installed_library_raw)
                    print(f"Response: {response}")
                    restart_flag = True
        if restart_flag is True:
            api_object.restart_cluster()
            time.sleep(10)
        for library_to_install in libraries_to_install:
            response = api_object.install_library_pip(library_to_install)
            print(f"response: {response}")


def upload_init_script(
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
    cfg = read_cfg(ENVIRONMENT_NAME, cfg_path)
    api_object = DatabricksRequest(cfg.get("databricks_host"), None, databricks_token)
    if init_script_dbfs_path[-1] != "/":
        init_script_dbfs_path += "/"
    dbfs_path = init_script_dbfs_path + init_script_local_path.split("/")[-1]
    upload_response = api_object.upload_file_dbfs(init_script_local_path, dbfs_path)
    if upload_response == dict():
        print(f"Package has been successfully installed.")


def upload_notebooks(
    cfg_path: str,
    secret_path: str,
    notebooks_artifact_path: str,
    notebooks_target_dir: str = "/cd_deployed/notebooks/",
):
    """
    Workflow for uploading notebooks to Databricks workspace.
    """
    databricks_token = read_token_from_file(secret_path)
    cfg = read_cfg(ENVIRONMENT_NAME, cfg_path)
    api_object = DatabricksRequest(cfg.get("databricks_host"), None, databricks_token)

    if notebooks_target_dir[0] != "/":
        notebooks_target_dir = "/" + notebooks_target_dir
    if notebooks_target_dir[-1] != "/":
        notebooks_target_dir += "/"

    notebook_paths = {
        "python_paths": [],
        "python_db_paths": [],
        "sql_paths": [],
        "sql_db_paths": [],
    }
    for entity in glob.glob(f"{notebooks_artifact_path}/**/*.py"):
        notebook_paths["python_db_paths"].append(
            notebooks_target_dir + "/".join(entity.split("/")[-2:])
        )

        notebook_paths["python_paths"].append(Path(entity))
    for entity in glob.glob(f"{notebooks_artifact_path}/**/*.sql"):
        if "/".join(entity.split("/")[-2:]).count(".") > 1:
            notebook_paths["sql_db_paths"].append(
                notebooks_target_dir
                + ".".join("/".join(entity.split("/")[-2:]).split(".")[:-1])
            )
        else:
            notebook_paths["sql_db_paths"].append(
                notebooks_target_dir + "/".join(entity.split("/")[-2:]).split(".")[0]
            )
        notebook_paths["sql_paths"].append(Path(entity))
    notebook_db_paths_concat = (
        notebook_paths["python_db_paths"] + notebook_paths["sql_db_paths"]
    )
    print(f"notebook_db_paths_concat: {notebook_db_paths_concat}")
    for x in range(len(notebook_db_paths_concat)):
        subdir = "/".join(notebook_db_paths_concat[x].split("/")[:-1])
        notebooks_dir_status = api_object.check_if_notebook_dir_exists(subdir)
        print(f"notebooks_dir_status: {notebooks_dir_status}")
        if notebooks_dir_status.get("error_code") == "RESOURCE_DOES_NOT_EXIST":
            print("This path does not exist - proceed to creating a directory")
            response = api_object.create_directory(subdir)
            if len(response) == 0:
                print(f"Directory has been created successfully.")
            else:
                print(f"Directory was not created - response from API:\n{response}")
    print(f"Debug notebook paths:\n{notebook_paths}")
    for k in notebook_paths.keys():
        print(f"Iterating over notebook_paths keys;\ncurrent key: {k}")
        if k == "python_paths":
            for x in range(len(notebook_paths[k])):
                print(
                    f'{notebook_paths["python_paths"][x]}\n{notebook_paths["python_db_paths"][x]}\n'
                )
                response = api_object.upload_notebooks(
                    notebook_paths["python_paths"][x],
                    notebook_paths["python_db_paths"][x],
                    "PYTHON",
                )
                print(response)
                if response == dict():
                    print(
                        f"Notebooks have been successfully uploaded to the path:\n"
                        f"{notebook_paths['python_db_paths'][x]}"
                    )
        elif k == "sql_paths":
            for x in range(len(notebook_paths[k])):
                print(
                    f'{notebook_paths["sql_paths"][x]}\n{notebook_paths["sql_db_paths"][x]}\n'
                )
                response = api_object.upload_notebooks(
                    notebook_paths["sql_paths"][x],
                    notebook_paths["sql_db_paths"][x],
                    "SQL",
                )
                print(response)
                if response == dict():
                    print(
                        f"Notebooks have been successfully uploaded to the path:\n"
                        f"{notebook_paths['sql_db_paths'][x]}"
                    )
