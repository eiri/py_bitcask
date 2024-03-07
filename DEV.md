# Dev notes

## Starting new project

### Getgo

```bash
poetry new -n --src <project>
cd <project>
asdf local python 3.12
poetry config virtualenvs.in-project true
poetry env use python
poetry shell
poetry add ruff pytest pre-commit
```

### Poetry config addition

```toml
[tool.ruff]
line-length = 80

[tool.ruff.lint]
select = [
    "E",  # pycodestyle errors (settings from FastAPI, thanks, @tiangolo!)
    "W",  # pycodestyle warnings
    "F",  # pyflakes
    "I",  # isort
    "C",  # flake8-comprehensions
    "B",  # flake8-bugbear
]
ignore = [
    "E501",  # line too long, handled by black
    "C901",  # too complex
]

[tool.ruff.lint.per-file-ignores]
"__init__.py" = ["F401"]

[tool.ruff.lint.isort]
order-by-type = true
relative-imports-order = "closest-to-furthest"
extra-standard-library = ["typing"]
section-order = ["future", "standard-library", "third-party", "first-party", "local-folder"]
known-first-party = []
```

### .gitignore

```bash
curl -s https://raw.githubusercontent.com/github/gitignore/master/Python.gitignore >> .gitignore
```

### pre-hook

In `.pre-commit-config.yaml`

```bash
repos:
-   repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
    -   id: trailing-whitespace
    -   id: end-of-file-fixer
    -   id: check-yaml
    -   id: check-added-large-files
-   repo: https://github.com/compilerla/conventional-pre-commit
    rev: v3.1.0
    hooks:
    -   id: conventional-pre-commit
        name: conventional-commit-check
        stages: [commit-msg]
        args: []
-   repo: https://github.com/python-poetry/poetry
    rev: '1.7.1'
    hooks:
    -   id: poetry-check
        name: poetry-check
-   repo: local
    hooks:
    -   id: ruff-check
        name: ruff-check
        entry: ruff check --fix --exit-non-zero-on-fix
        language: system
        types: [python]
    -   id: ruff-format
        name: ruff-format
        entry: ruff format
        language: system
        types: [python]
```

```bash
pre-commit install
pre-commit autoupdate
```

### github actions

In `.github/workflows/tests.yml`

```yaml
name: tests

on:
  push:
    branches: ['main']
  pull_request:
    branches: ['main']

jobs:
  tests:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - name: Setup python
      uses: actions/setup-python@v5
      with:
        python-version: '3.12'
    - name: Install poetry
      run: python -m pip install poetry
    - name: Configure poetry
      run: poetry config virtualenvs.in-project true
    - name: Cache the virtualenv
      uses: actions/cache@v4
      with:
        path: ./.venv
        key: ${{ runner.os }}-venv-${{ hashFiles('**/poetry.lock') }}
    - name: Install dependencies
      run: poetry install
    - name: Lint
      run: poetry run ruff check --show-files --exit-non-zero-on-fix
    - name: Run tests
      run: poetry run pytest -v --cov --cov-report term-missing
    - name: Build artifacts
      run: poetry build
```
ππ
### vscode

While in poetry shell call `code .` then cmd+shift+p "Python: select interpreter".

Then Cmd+P select `settings.json` in `.vscode` and add

```json
{
    "[python]": {
        "editor.rulers": [72, 79],
        "editor.formatOnSave": true,
        "editor.defaultFormatter": "charliermarsh.ruff",
        "editor.codeActionsOnSave": {
            "source.fixAll": "always",
            "source.organizeImports": "always"
        }
    },
    "python.testing.pytestArgs": [
        "tests"
    ],
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true
}
```
