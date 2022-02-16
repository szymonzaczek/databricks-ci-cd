import sys
from find_files import find_files_job, find_files_in_nested_dir_job

cli_args = sys.argv[1:]
allowed_first_cli_args = ["find_files_job", "find_files_in_nested_dir_job"]

if cli_args[0] not in allowed_first_cli_args:
    raise ValueError("Invalid CLI command. Use one of %s" % allowed_first_cli_args)

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