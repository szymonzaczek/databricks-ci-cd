import json


def read_cfg(env: str, cfg_file: str) -> dict:
    """
    Reading standard cfg from JSON file.
    The actual values for each key used in the deployment are partitioned by a given
    environment.

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
    return cfg