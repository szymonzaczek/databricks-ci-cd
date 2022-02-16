import sys
from process_requirements_locally import process_requirements

requirements_variable = sys.argv[1:][0]

if len(sys.argv[1:]) > 1:
    requirements_type = sys.argv[1:][1]
else:
    requirements_type = "dev"

process_requirements(requirements_variable, requirements_type)