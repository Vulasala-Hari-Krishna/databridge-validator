# Copilot Instructions — databridge-validator

## Project Overview

`databridge-validator` is a **Python library** published on **PyPI** (`pip install databridge-validator`).
It provides generic, reusable utilities for **validating data migration** from legacy systems
(mainframe COBOL, DB2, flat files) to modern cloud platforms (AWS Aurora, S3, Redshift, etc.).

Users supply **source** and **target** DataFrames (pandas or PySpark). The library compares them
and generates structured validation reports: mismatches, source-only extras, target-only extras,
summary statistics, and optionally masks PII columns in reports.

**This is a library, NOT an application.** It has no CLI, no main entry point, no config files.
Users import functions and call them in their own scripts/jobs.

---

## Package Identity

- **PyPI name:** `databridge-validator`
- **Python import:** `databridge_validator`
- **Python version:** >=3.9
- **Required dependency:** pandas
- **Optional dependency:** pyspark (Spark DataFrame support)

---

## Target Project Structure

```
src/
└── databridge_validator/
    ├── __init__.py                  # Public API surface — all user-facing functions exported here
    ├── _typing.py                   # Internal type aliases (DataFrameType, etc.)
    ├── core/
    │   ├── __init__.py
    │   ├── comparator.py            # Core comparison engine: delta detection via hashing
    │   ├── reporter.py              # Report builders: mismatch, extras, summary
    │   └── models.py                # Result dataclasses (ValidationResult, SummaryReport, etc.)
    ├── cleaning/
    │   ├── __init__.py
    │   └── sanitizer.py             # DataFrame cleaning: trim, regex clean, null normalization
    ├── pii/
    │   ├── __init__.py
    │   └── masking.py               # PII masking strategies: alternate-char, hash, redact, partial
    └── utils/
        ├── __init__.py
        └── dataframe_helpers.py     # Shared helpers: type detection, column normalization, casting
tests/
├── conftest.py                      # Shared fixtures: sample DataFrames, mock Spark sessions
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

## Core Engineering Principles

1. **TDD mandatory**: Write a failing test → implement → refactor. Every feature starts with a test.
2. **DRY everywhere**: No duplication in production OR test code. Extract shared logic into `utils/`.
3. **Coverage ≥80%**: On all new/changed code. CI enforces this.
4. **Dual DataFrame support**: Every data function MUST support both pandas and PySpark DataFrames.
   - Use runtime `isinstance()` checks to branch logic
   - PySpark is an **optional** dependency — code must not crash if PySpark is not installed
5. **Never mutate inputs**: Always return new DataFrames/objects. User's data stays untouched.
6. **User-friendly API**: Sensible defaults for all optional parameters. Users should be able to call
   functions with minimal arguments and get useful results.
7. **Type hints + docstrings**: All public functions must have complete type annotations and
   Google-style docstrings with Args, Returns, Raises, and Example sections.
8. **Security first**: No hardcoded secrets, paths, or environment-specific config. Validate all inputs.
9. **Minimal dependencies**: pandas is the only required dependency. PySpark is optional. Do not add
   new dependencies without strong justification.

---

## Existing Legacy Code Reference

The following functions exist in a rough `validation_util.py` script and need to be **redesigned,
refactored, and properly structured** into the package:

### Functions to Modernize

| Legacy Function | Target Module | Refactoring Notes |
|---|---|---|
| `mask_col_udf` / `mask()` | `pii/masking.py` | Rename to `mask_alternate_chars()`. Support both pandas Series and PySpark columns. Add strategy parameter. |
| `mask_pii()` + `mask_pii_udf` | `pii/masking.py` | Decouple from Spark UDF. Make generic for both DataFrame types. |
| `get_mismatch_report()` | `core/reporter.py` | Rename to `build_mismatch_report()`. Make `pii_cols`, `pii_exists`, `acceptable_mismatch_columns` optional with defaults. Return structured `ValidationResult`. |
| `trim_spaces_all_column()` | `cleaning/sanitizer.py` | Rename to `trim_whitespace()`. Support pandas + Spark. |
| `clean_dataframe()` | `cleaning/sanitizer.py` | Rename to `clean_control_characters()`. Make regex pattern configurable. Support pandas + Spark. |
| `get_delta_reports()` | `core/comparator.py` | Rename to `compare_dataframes()`. Make `num_of_partitions`, `persist` behavior, `copybook_files`, `args` optional/configurable. Return `ValidationResult` dataclass. |

### Key Design Issues in Legacy Code to Fix

- **Tight coupling to PySpark**: Functions use `F.udf()`, `col()`, `lit()` etc. at module level — breaks when PySpark not installed
- **No pandas support**: All functions are Spark-only
- **Hardcoded `print()` for errors**: Replace with proper `logging` module
- **No return type structure**: Functions return raw DataFrames — should return typed result objects
- **Too many required parameters**: `get_delta_reports()` takes 8 params, many should be optional
- **No input validation**: No checks for DataFrame types, empty DataFrames, missing columns
- **`Stringtype()` typo**: Should be `StringType()` — indicates lack of testing
- **Excessive `.persist()`**: Should be user-configurable, not hardcoded
- **`regex_replace` typo**: Should be `regexp_replace`

---

## Coding Standards

- Python 3.9+ syntax (use `Union[]` from typing, not `X | Y` pipe syntax for 3.9 compat)
- `ruff` for linting and formatting (line length 120)
- `pytest` + `pytest-cov` for testing
- `mypy` for type checking (optional but encouraged)
- Google-style docstrings
- `__all__` in every `__init__.py` to control public API
- Semantic versioning (MAJOR.MINOR.PATCH)
- Conventional commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`

---

## Public API Design Principles

```python
# Users should be able to do this with minimal code:
from databridge_validator import compare_dataframes, clean_dataframe, mask_pii_columns

# Simple usage — sensible defaults
result = compare_dataframes(source_df, target_df, key_columns=["id"])
print(result.summary)
print(result.mismatch_records)

# Advanced usage — full control
result = compare_dataframes(
    source_df=source_df,
    target_df=target_df,
    key_columns=["id", "date"],
    exclude_columns=["updated_at"],
    pii_columns=["ssn", "email"],
    mask_pii=True,
    num_partitions=200,            # Spark only, optional
    persist_intermediates=True,     # Spark only, optional
    case_sensitive=False,
)
```

---

## What NOT to Do

- Do NOT add CLI/entry points — this is a library
- Do NOT read files or connect to databases — that's the user's job
- Do NOT create real SparkSessions in unit tests
- Do NOT use `print()` — use `logging` module
- Do NOT put logic in `__init__.py` — only imports and `__all__`
- Do NOT add heavy dependencies (no requests, no boto3, no sqlalchemy)
- Do NOT use mutable default arguments