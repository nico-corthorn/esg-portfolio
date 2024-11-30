from setuptools import setup, find_packages
import os

def read_requirements(filename):
    # Get the directory where setup.py is located (project root)
    root_dir = os.path.dirname(os.path.abspath(__file__))
    requirements_path = os.path.join(root_dir, filename)
    
    with open(requirements_path) as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]


if __name__ == "__main__":
    setup(
        name="esgtools",
        packages=find_packages(include=['esgtools', 'esgtools.*']),
        install_requires=read_requirements("lambda_requirements.txt")
    )