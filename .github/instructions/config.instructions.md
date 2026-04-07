---
applyTo: "pyproject.toml,*.cfg,*.ini,.github/workflows/*.yml"
---

# Configuration & CI/CD Instructions

## pyproject.toml

- Build system: `hatchling`
- Package name: `databridge-validator`
- Import name: `databridge_validator`
- Minimum Python: `>=3.9`
- Required dependencies: `pandas>=1.5.0`
- Optional dependency groups:
  - `spark`: `pyspark>=3.3.0`
  - `dev`: `pytest>=7.0`, `pytest-cov>=4.0`, `ruff>=0.4`, `build>=1.0`, `twine>=5.0`, `mypy>=1.0`
  - `all`: includes spark + dev
- Hatch build targets must specify `packages = ["src/databridge_validator"]`
- Version follows strict semver: MAJOR.MINOR.PATCH
- Classifiers: Python 3, MIT License, OS Independent, relevant topic classifiers

## ruff configuration (in pyproject.toml)

```toml
[tool.ruff]
line-length = 120
target-version = "py39"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "UP", "B", "SIM", "RUF"]

[tool.ruff.format]
quote-style = "double"
```

## pytest configuration (in pyproject.toml)

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-v --tb=short"
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    "spark: marks tests requiring PySpark",
]
```

## GitHub Actions Workflows

### CI workflow (ci.yml) — runs on every push and PR to main
- Matrix: Python 3.9, 3.10, 3.11, 3.12
- Steps: checkout → setup-python → install deps → ruff lint → ruff format check → pytest with coverage → upload coverage
- Coverage must be ≥80% or the job fails

### Publish workflow (publish.yml) — runs on version tags (v*.*.*)
- Uses PyPI Trusted Publishing (OIDC) — no API tokens needed
- Steps: checkout → setup-python → build → twine check → publish via `pypa/gh-action-pypi-publish`
- Must have `permissions: id-token: write`