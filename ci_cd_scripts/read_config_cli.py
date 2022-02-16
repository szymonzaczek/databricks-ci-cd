import sys
import os
from read_config import read_cfg

# CAVEAT: setting up env variable for CI part of the pipeline
if os.environ.get("SYSTEM_STAGEDISPLAYNAME") == "__default":
    SYSTEM_STAGEDISPLAYNAME = "dv"
else:
    SYSTEM_STAGEDISPLAYNAME = os.environ.get("SYSTEM_STAGEDISPLAYNAME")

cfg = read_cfg(SYSTEM_STAGEDISPLAYNAME, sys.argv[1:][0])

for k, v in cfg.items():
    if k == "databricks_cluster_id" and isinstance(v, list):
        print(f"##vso[task.setvariable variable={k}]{v[0]}")
    else:
        print(f"##vso[task.setvariable variable={k}]{v}")