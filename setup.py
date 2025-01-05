from setuptools import setup, find_packages

def read_requirements(filename):
    with open(filename) as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name="esgtools",
    version="0.0.1",
    packages=find_packages(include=['esgtools*']),  # Explicitly include all esgtools subpackages
    package_data={'esgtools': ['**/*']},  # Include all files in the package
    include_package_data=True,
    install_requires=read_requirements('lambda_requirements.txt'),
)