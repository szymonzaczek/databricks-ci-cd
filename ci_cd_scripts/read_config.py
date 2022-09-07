import json


def read_cfg(env: str, cfg_file: str, export_to_task_variables: bool = True) -> dict:
    """
    Reading standard cfg from JSON file.
    The actual values for each key used in the deployment are partitioned by a given
    environment.

        {
    "databricks_host": {
        "dev": "https://adb-xx.x.azuredatabricks.net/",
        "stg": "https://adb-xy.y.azuredatabricks.net/",
        "prd": "https://adb-yy.z.azuredatabricks.net/"
    },
    "databricks_cluster_id": {
        "dev": "xx",
        "stg": "xy",
        "prd": "yy"
    }
    }

    :param env: deployment environment (e.g. dev, stg, prd)
    :type env: str
    :param cfg_file: relative path to the config file
    :type cfg_file: str
    :return: parsed configuration
    :rtype: dict
    """
    file = open(cfg_file)
    whole_cfg = json.load(file)
    file.close()
    cfg_keys = [*whole_cfg.keys()]
    cfg = dict()
    for x in cfg_keys:
        cfg[x] = whole_cfg.get(x).get(env)
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