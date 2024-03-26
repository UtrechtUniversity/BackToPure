from setuptools import setup, find_packages

setup(
    name='import functions for pure',
    version='0.1.0',
    packages=find_packages(where="src"),  # Tells setuptools to find packages in 'src' directory
    package_dir={"": "src"},  # Root package directory is 'src'

)
