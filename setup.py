from setuptools import setup, find_packages

setup(
    name="who_covers",
    version="0.1.0",
    package_dir={"":"src"},
    packages=find_packages("src"),
    install_requires=[
          "pandas>=2.1",
          "pyarrow>=15.0",
          "cfbd>=4.6.8",
          "numpy>=1.26",
    ]
)
