# Project overview

Lorelie is a Python package that provides APIs to interact with SQlite databases. It is designed to be easy to use and efficient, making it a great choice for developers who need to work with SQLite databases in their Python applications.

* The source code is available under the [src/lorelie](src/lorelie) directory and contains three main modules: `main`, `exceptions`, and `typings`.
* You can find the documentation for the package in the [README](README.md) file, which includes installation instructions, usage examples, and API reference.

# Commands

## Environment setup

* Create a virtual environment using `uv` ([uv documentation](https://docs.astral.sh/uv/)): `uv venv .venv`
* Sync the dependencies: `uv sync` then `uv lock` (if you want to update the lock file)
* Activate the virtual environment: `source .venv/bin/activate` (Linux/Mac) or `.venv\Scripts\activate` (Windows)

## Build

* Build the package: `uv build --no-sources`
* Check the version of the package with: `uv version`

# Code style guidelines

* Use the latest Python version 3.x and the latest PEP 8 style guide for Python code ([PEP 8](https://www.python.org/dev/peps/pep-0008/)).

# Testing instructions

* Run the tests: `pytest`

# Security considerations
