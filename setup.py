"""Setup for Prometheus Api Client module."""
import os
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


def get_install_requires():
    """Get requirements from requirements.txt."""
    with open("requirements.txt", "r") as requirements_file:
        res = requirements_file.readlines()
        return [req.split(" ", maxsplit=1)[0] for req in res if req]


def get_version():
    """Get package version."""
    with open(os.path.join("aioprometheus_api_client", "__init__.py")) as f:
        content = f.readlines()

    for line in content:
        if line.startswith("__version__ ="):
            # dirty, remove trailing and leading chars
            return line.split(" = ")[1][1:-2]
    raise ValueError("No version identifier found")


VERSION = get_version()
setuptools.setup(
    name="aioprometheus-api-client",
    version=VERSION,
    author="Tri Songz",
    author_email="asanmukhani@microsoft.com",
    description="An async python api to collect data from prometheus",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/trisongz/aioprometheus-api-client",
    install_requires=get_install_requires(),
    packages=setuptools.find_packages(),
    tests_require=["httmock"],
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
