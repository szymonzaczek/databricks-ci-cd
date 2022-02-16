import sys
import os
from pathlib import Path
from create_init_script import build_script_content, create_init_script


working_dir = Path(sys.argv[1:][0])
init_script_local_path = Path(sys.argv[1:][1])
package_dbfs_dir = Path(sys.argv[1:][2])
whl_files_variable = sys.argv[1:][3]
requirements_variable = sys.argv[1:][4]
os.chdir(str(working_dir))

whl_files = whl_files_variable.split(",")
requirements = requirements_variable.split(",")

requirements_files = []
for req_file in requirements:
    # init script is always for worklads, so it will never use dev.txt requirements
    if req_file.split("/")[-1] == "common.txt":
        requirements_files.append(req_file)

requirements_files = tuple(requirements_files)

init_script = build_script_content(str(package_dbfs_dir), requirements_files, *whl_files)
create_init_script(init_script, str(init_script_local_path))
