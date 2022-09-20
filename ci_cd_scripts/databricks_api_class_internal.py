import requests
import os
import json
import base64
from typing import Union


ENVIRONMENT_NAME = os.environ.get("ENVIRONMENT_NAME")
print(f"Environment name: {ENVIRONMENT_NAME}")
BUILD_REPOSITORY_NAME = os.environ.get("BUILD_REPOSITORY_NAME")

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

    def check_current_cluster_status(self, cluster_details):
        """
        Check the status of the cluster. Available statuses:
        ["PENDING", "RUNNING, "RESTARTING, "RESIZING", "TERMINATING", "TERMINATED"]

        Argument - output of self.get_cluster_details()
        """
        return cluster_details.get("state")

    def start_cluster(self) -> str:
        """
        Start the cluster.
        """
        url = self.url + "clusters/start"
        response = requests.post(url, headers=self.headers, json=self.payload)
        return response.text

    def restart_cluster(self) -> str:
        """
        Restart the cluster
        """
        url = self.url + "clusters/restart"
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

    def delete_file_dbfs(self, path: str) -> str:
        """
        Delete a file from DBFS.
        """
        url = self.url + "dbfs/delete"
        payload = {"path": path, "recursive": True}
        response = requests.post(url, headers=self.headers, json=payload)
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

    def get_directory_info(self, dir_path: str, api_version: str = "2.0"):
        """
        Get info about a directory.
        """
        url = self.host + f"api/{api_version}/workspace/get-status"
        payload = {"path": dir_path}
        response = requests.get(url, headers=self.headers, json=payload)
        return json.loads(response.text)

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


