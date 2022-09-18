import json
from typing import Union


def read_env_cfg(
    env: str,
    cfg_file: str,
    output_file: str = None,
    export_to_task_variables: bool = True,
) -> dict:
    """
    Reading standard cfg from JSON file.
    The actual values for each key used in the deployment are partitioned by a given
    environment.
    An example of a standard JSON cfg file:

    {
    "databricks_host": {
        "dev": "https://adb-4164709695360957.17.azuredatabricks.net/",
        "lab": "https://adb-4164709695360957.17.azuredatabricks.net/",
        "prd": "https://adb-1614347014667080.0.azuredatabricks.net/"
    },
    "databricks_cluster_id": {
        "dev": "0819-133744-wake392",
        "lab": "0819-133744-wake392",
        "prd": "1208-104247-goo5piqa"
    }
    }

    :param env: deployment environment (e.g. dev, lab, prd)
    :type env: str
    :param cfg_file: relative path to the config file
    :type cfg_file: str

    :return: parsed configuration
    :rtype: dict
    """
    file = open(cfg_file)
    whole_cfg = json.load(file)
    file.close()
    print(f"env: {env}")
    print(f"whole cfg: {whole_cfg}")
    cfg_keys = [*whole_cfg.keys()]
    cfg = dict()
    for x in cfg_keys:
        cfg[x] = whole_cfg.get(x).get(env)
    print(f"Parsed config: {cfg}")
    if output_file:
        with open(output_file, "w") as f:
            json.dump(cfg, f)
    if export_to_task_variables:
        export_dict_to_task_variables(cfg)
    return cfg


def read_flat_cfg(cfg_file: str, export_to_task_variables: bool = True) -> dict:
    """
    Reading flat config from json file and outputting its content to dictionary.

    :param env: deployment environment (e.g. dev, lab, prd)
    :type env: str
    :param cfg_file: relative path to the config file
    :type cfg_file: str

    :return: parsed configuration
    :rtype: dict
    """
    file = open(cfg_file)
    cfg = json.load(file)
    file.close()
    print(f"cfg: {cfg}")
    print(f"cfg type: {type(cfg)}")
    if export_to_task_variables:
        export_dict_to_task_variables(cfg)
    return cfg


def export_dict_to_task_variables(cfg: dict) -> None:
    """
    Exporting dict to task variables on Azure's agent.

    :param cfg: parsed configuration
    :type cfg: dict
    """
    for k, v in cfg.items():
        if k == "databricks_cluster_id" and isinstance(v, list):
            print(f"##vso[task.setvariable variable={k}]{v[0]}")
        else:
            print(f"##vso[task.setvariable variable={k}]{v}")


def export_string_to_task_variables(
    variable_name: str, value: Union[str, bool], is_output: bool = False
) -> None:
    """
    Exporting string to task variables on Azure's agent.

    :param variable_name: name of the task variable
    :type variable_name: str
    :param value: value of the task variable
    :type value: str
    """
    print(f"Creating task variable: {variable_name} with value: {value}")
    print(
        f"##vso[task.setvariable variable={variable_name};isOutput={str(is_output).lower}]{value}"
    )
