# AGENTS.md — databridge-validator

## Identity

This is a **Python library** for PyPI publication. Package: `databridge-validator`. Import: `databridge_validator`.

It validates data migration from legacy systems to cloud by comparing source and target DataFrames
(pandas or PySpark) and producing structured reports.

---

## CRITICAL RULES — Always Follow These

1. **TDD is non-negotiable**: Create failing test FIRST, then implement. Never write code without a test.
2. **Never break public API**: Existing function signatures are contracts. Add params with defaults only.
3. **Dual DataFrame support**: Every data function supports pandas AND PySpark. PySpark is optional (lazy imports).
4. **Never mutate input DataFrames**: Always work on copies/new objects.
5. **No hardcoded values**: No file paths, credentials, connection strings, environment config.
6. **Minimal dependencies**: Only `pandas` is required. PySpark optional. No new deps without justification.
7. **No CLI / no main()**: This is a library. Users import functions.
8. **Coverage ≥80%**: Every PR must maintain this. Run `pytest --cov=databridge_validator --cov-fail-under=80`.

---

## Build & Test Commands

```bash
# Install in development mode with all optional deps
pip install -e ".[dev,spark]"

# Run full test suite with coverage enforcement
pytest --cov=databridge_validator --cov-fail-under=80 -v

# Run only fast unit tests
pytest tests/unit/ -v

# Run integration tests (requires PySpark)
pytest tests/integration/ -v -m spark

# Lint
ruff check src/ tests/

# Format check
ruff format --check src/ tests/

# Type check
mypy src/

# Build package
python -m build

# Validate package before upload
twine check dist/*
```

---

## Project Structure

```
src/databridge_validator/
├── __init__.py                     # Public API: __all__ with all user-facing functions
├── _typing.py                      # Internal type aliases
├── core/
│   ├── __init__.py
│   ├── comparator.py               # compare_dataframes() — main comparison engine
│   ├── reporter.py                 # build_mismatch_report(), build_summary_report()
│   └── models.py                   # ValidationResult, SummaryReport dataclasses
├── cleaning/
│   ├── __init__.py
│   └── sanitizer.py                # trim_whitespace(), clean_control_characters()
├── pii/
│   ├── __init__.py
│   └── masking.py                  # mask_pii_columns(), mask_alternate_chars()
└── utils/
    ├── __init__.py
    └── dataframe_helpers.py        # _is_spark_dataframe(), normalize_columns(), cast_all_to_string()

tests/
├── conftest.py                     # Shared fixtures: sample DataFrames
├── unit/
│   ├── test_comparator.py
│   ├── test_reporter.py
│   ├── test_sanitizer.py
│   ├── test_masking.py
│   ├── test_models.py
│   └── test_dataframe_helpers.py
└── integration/
    ├── test_full_validation_pandas.py
    └── test_full_validation_spark.py
```

---

## Legacy Code Being Modernized

The following functions from a legacy `validation_util.py` are being refactored into this package.
Reference this when implementing to preserve core logic while fixing design issues:

### `get_delta_reports()` → `compare_dataframes()`
**What it does**: Hashes all columns, performs left-anti joins to find deltas, then calls mismatch reporter.
**Issues to fix**:
- 8 required parameters → make most optional with defaults
- Hardcoded `persist(MEMORY_AND_DISK)` → make configurable: `persist: bool = False`
- Hardcoded `num_of_partitions` required → make optional: `num_partitions: Optional[int] = None`
- `copybook_files` and `args` params → remove (not relevant to generic library)
- Returns raw tuple of DataFrames → return `ValidationResult` dataclass
- Only supports Spark → add pandas implementation
- Uses `print()` for errors → use `logging`

### `get_mismatch_report()` → `build_mismatch_report()`
**What it does**: Joins source/target on keys, builds per-column mismatch strings like `{col : (src_val:tgt_val)}`, filters non-empty.
**Issues to fix**:
- `pii_exists` boolean is redundant → derive from `pii_columns is not None`
- `acceptable_mismatch_columns` → rename to `exclude_columns`
- Hardcoded `row_sha2` column name → use constant or configurable name
- `Stringtype()` typo → `StringType()`
- Only Spark → add pandas version
- Returns raw DataFrame → integrate into `ValidationResult`

### `mask()` / `mask_col_udf()` → `mask_alternate_chars()`
**What it does**: Masks every other character with `*`. E.g., "Hello" → "H*l*o"
**Issues to fix**:
- `mask_col_udf` uses regex which is slower and less readable → use enumerate like `mask()` does
- Consolidate into one clean function
- Add multiple strategies: "alternate", "hash", "redact", "partial"
- Support pandas Series and PySpark Column

### `mask_pii()` → integrate into `mask_pii_columns()`
**What it does**: Parses mismatch string format `{col : (val1:val2)}` and masks values for PII columns.
**Issues to fix**:
- Tightly coupled to the specific string format from `get_mismatch_report()`
- Should work on any DataFrame column, not just mismatch strings
- Add option to mask entire columns in a DataFrame (not just mismatch reports)

### `trim_spaces_all_column()` → `trim_whitespace()`
**What it does**: Trims leading/trailing spaces from all string columns.
**Issues to fix**: Only Spark → add pandas. Better name.

### `clean_dataframe()` → `clean_control_characters()`
**What it does**: Removes control characters (\\r, \\n, \\t, etc.) from string columns.
**Issues to fix**:
- `regex_replace` typo → `regexp_replace`
- Hardcoded character list → make configurable with sensible default
- Only Spark → add pandas

---

## Additional Functions to Implement (Suggestions)

Beyond modernizing existing code, consider adding these utilities:

| Function | Module | Purpose |
|---|---|---|
| `get_schema_diff()` | `core/comparator.py` | Compare schemas/columns between source and target; report missing/extra/type-different columns |
| `get_row_counts()` | `core/reporter.py` | Quick count comparison: source count, target count, delta |
| `get_duplicate_report()` | `core/reporter.py` | Find duplicate rows based on key columns in either DataFrame |
| `get_null_analysis()` | `core/reporter.py` | Report null/empty counts per column for a DataFrame |
| `normalize_columns()` | `utils/dataframe_helpers.py` | Lowercase column names, strip whitespace, replace special chars |
| `cast_all_to_string()` | `utils/dataframe_helpers.py` | Cast all columns to string (for comparison purposes) |
| `export_report()` | `core/reporter.py` | Export ValidationResult to CSV, JSON, or dict |
| `mask_with_hash()` | `pii/masking.py` | SHA-256 hash masking for PII columns |
| `mask_partial()` | `pii/masking.py` | Show first/last N chars, mask middle (e.g., "J***e" for "Jane") |
| `validate_key_uniqueness()` | `utils/dataframe_helpers.py` | Check if key columns are unique in the DataFrame before comparison |

---

## Result Models

```python
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Union
import pandas as pd

@dataclass
class ValidationResult:
    """Structured result from a DataFrame comparison."""

    is_match: bool
    total_source_rows: int
    total_target_rows: int
    matched_count: int
    mismatch_count: int
    source_extra_count: int
    target_extra_count: int
    mismatch_records: Optional[Union[pd.DataFrame, Any]] = None   # Any = SparkDataFrame
    source_extra_records: Optional[Union[pd.DataFrame, Any]] = None
    target_extra_records: Optional[Union[pd.DataFrame, Any]] = None
    summary: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to a dictionary (excludes DataFrames)."""
        ...

    def __str__(self) -> str:
        """Human-readable summary string."""
        ...
```

---

## Workflow for Adding a New Feature

1. **Write failing tests** in `tests/unit/test_<module>.py` (both pandas and spark if applicable)
2. **Run tests** — confirm they fail: `pytest tests/unit/test_<module>.py -v`
3. **Implement** minimal code in `src/databridge_validator/<module>/`
4. **Run all tests**: `pytest --cov=databridge_validator --cov-fail-under=80 -v`
5. **Lint**: `ruff check src/ tests/ && ruff format --check src/ tests/`
6. **Export**: Add public functions to `__init__.py` and `__all__`
7. **Document**: Docstring on function + update README.md with usage example
8. **Commit**: Use conventional commit format: `feat: add schema comparison utility`

---

## Things to NEVER Do

- ❌ Create real SparkSessions in unit tests (use mocks or skip)
- ❌ Read/write files in unit tests
- ❌ Use `print()` anywhere (use `logging`)
- ❌ Put implementation logic in `__init__.py`
- ❌ Add CLI entry points
- ❌ Add heavy dependencies (requests, boto3, sqlalchemy, etc.)
- ❌ Use mutable default arguments (`def f(x=[])`)
- ❌ Use bare `except:` or `except Exception:`
- ❌ Hardcode column names like `"row_sha2"` — use constants
- ❌ Assume PySpark is installed — always use lazy imports
- ❌ Mutate user's input DataFrames
- ❌ Return raw DataFrames from comparison functions — use `ValidationResult`