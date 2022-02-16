import sys
from copy_files import copy_requirements, copy_files

cli_args = sys.argv[1:]

if len(cli_args) == 1:
    evaluation_string = f"{cli_args[0]}()"
    print(f"evaluation_string: {evaluation_string}")
    eval(evaluation_string)
elif len(cli_args) == 2:
    evaluation_string = f"{cli_args[0]}('{cli_args[1]}')"
    print(f"evaluation_string: {evaluation_string}")
    eval(evaluation_string)
elif len(cli_args) > 2:
    evaluation_string = f"{cli_args[0]}{*cli_args[1:],}"
    print(f"evaluation_string: {evaluation_string}")
    eval(evaluation_string)