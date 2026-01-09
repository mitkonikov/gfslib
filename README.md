# GeMMA Fusion Suite Python Library (GFSLib)

GFSLib is a Python library that provides utilities for working with the GeMMA Fusion Suite Server.
It is currently in alpha development and only includes storage utilities for communicating with the GF Server.

## Getting Started

To install GFSLib, use pip:

```bash
pip install gfslib
```

## Example Usage

Here is a simple example of how to use the storage utilities in GFSLib:

```python
from gfslib.storage import StorageServices

storage = StorageServices("https://.../api/ws/<workspace-id>/services/storage")
storage.set_api_key("...")

storage.upload("path/to/remote.file", "/path/to/local.file")
storage.download("path/to/remote.file", "/path/to/downloaded.file")
```

## Contributions

We really appreciate contributions from the community!
We especially welcome the reports of issues and bugs.

However, one may note that since this library is currently being heavily developed,
the API may drastically change and all projects depending on this library have to deal
with the changes downstream. We will however try to keep these at minimum.

The main maintainer of this library is [Mitko Nikov](https://github.com/mitkonikov).

### Developing the library

We are using [poetry](https://python-poetry.org/) to manage, build and publish the python package.
We recommend downloading poetry and running `poetry install` to
install all of the dependencies instead of doing so manually.

To activate the virtual env created by poetry, run `poetry env activate` to get the
command to activate the env. After activation, you can run anything from within.

### Contributing to GitHub

There are three things that we are very strict about:
 - Type-checking - powered by [mypy](https://mypy-lang.org/)
 - Coding style - powered by [Black](https://black.readthedocs.io/en/stable/)
 - Unit Tests - powered by [pytest](https://docs.pytest.org/en/stable/)

Run the following commands in the virtual env
to ensure that everything is according to the guidelines:

```sh
mypy . --strict
black .
pytest .
```

Guidelines are now checked using GitHub Workflows.
When developing the library locally, you can install [act](https://nektosact.com/) to run
the GitHub workflows on your machine through Docker.
We also recommend installing the VSCode extension
[GitHub Local Actions](https://marketplace.visualstudio.com/items?itemName=SanjulaGanepola.github-local-actions)
to run the workflows from inside VSCode, making the process painless.

Example scenarios are also tested in GitHub Actions by running them from the CLI.

## General Guidelines

Here are a few guidelines to following while contributing on the library:
 - We aim to keep this library with as little run-time-necessary dependencies as possible.
 - Unit tests for as many functions as possible. (we know that we can't cover everything)
 - Strict Static Type-checking using `mypy`
 - Strict formatting style guidelines using `black`
 - Nicely documented functions and classes
