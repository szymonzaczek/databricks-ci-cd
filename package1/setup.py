from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

# version numbers conform to "major.minor.micro" - major and minor are manually changed,
# micro is generated via  CI/CD pipeline: PEP440
__version__ = "0.1"

setup(
    name="databricks-ci-cd",
    author="Szymon Zaczek",
    author_email="szaczek@ecovadis.com",
    description=(
        "Dummy package"
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    version=__version__,
    packages=find_packages(),
    setup_requires=["flake8"],
)
