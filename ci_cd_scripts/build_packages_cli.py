import sys
from build_packages import process_setup_py

setup_py_variable = sys.argv[1:][0]

process_setup_py(setup_py_variable)