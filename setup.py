from setuptools import setup, find_packages

setup(
    name="finops_account_manager",
    version="0.1.0",
    packages=find_packages(),           # this auto-discovers your package
    install_requires=[
        "boto3",
        "pytest",
    ],
)
