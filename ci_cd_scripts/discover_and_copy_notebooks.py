import glob
import sys
import re
from shutil import copy
from pathlib import Path

working_dir = Path(sys.argv[1:][0])
subdir = sys.argv[1:][1]  # notebooks directory
target_dir = Path(sys.argv[1:][2])

notebooks_local_paths = []
notebooks_target_paths = []

for entity in glob.glob(f"{str(working_dir)}/*/{subdir}/*.py"):
    print(f"notebook files paths: {entity}")
    notebooks_local_paths.append(Path(entity))

for entity in glob.glob(f"{str(working_dir)}/*/{subdir}/*.sql"):
    print(f"notebook files paths: {entity}")
    notebooks_local_paths.append(Path(entity))

pattern = r".notebooks"

for x in range(len(notebooks_local_paths)):
    target_sub_path = Path(re.sub(pattern, "", notebooks_local_paths[x].__str__()))
    target_sub_path_relative = "/".join(str(target_sub_path).split("/")[-2:])
    final_path = Path(target_dir.__str__() + "/" + target_sub_path_relative.__str__())
    notebooks_target_paths.append(final_path)

for i, file in enumerate(notebooks_local_paths):
    dir_path = Path(
        "/".join(notebooks_target_paths[i].__str__().replace("\\", "/").split("/")[:-1])
    )
    dir_path.mkdir(parents=True, exist_ok=True)
    copy(file, notebooks_target_paths[i])
