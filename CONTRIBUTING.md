## How to contribute to Deta Python SDK

Thank you for considering contributing to Deta Python SDK!

### Submitting Updates

Include the following in your patch:

- Use [Black](https://github.com/psf/black) to format your code. This and other tools will run automatically if you install [pre-commit](https://github.com/pre-commit/pre-commit-hooks) using the instructions below.


- Use [mypy](https://github.com/python/mypy) to check static typing on the codebase.

- Write concise commit messages.

- Write proper description for the change you made. Screenshots or other visual cue can be helpful.

- Add/Update `tests` for the changes you made.

- Add/Update `README.md` based on the new changes.

### First time setup

- [Fork the project](https://github.com/deta)

- Clone the repo locally.

```bash
git clone https://github.com/<your-gh-username>/deta-python
```

- Create a virtualenv

```bash
python3 -m venv venv

# activate virtualenv
source venv/bin/activate
```

### Install Deta Python SDK in editable mode with development dependencies.

```bash
make setup
```

### Install the pre-commit hooks.

```bash
pre-commit install

pre-commit run
```

Start coding 🚀
