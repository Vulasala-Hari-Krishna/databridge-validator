# databridge-validator

[![PyPI version](https://badge.fury.io/py/databridge-validator.svg)](https://pypi.org/project/databridge-validator/)
[![Python versions](https://img.shields.io/pypi/pyversions/databridge-validator.svg)](https://pypi.org/project/databridge-validator/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Generic, reusable utilities for **validating data migration** from legacy systems
(mainframe COBOL, DB2, flat files) to modern cloud platforms (AWS Aurora, S3, Redshift, etc.).

Supply **source** and **target** DataFrames (pandas or PySpark). The library compares them
and generates structured validation reports: mismatches, source-only extras, target-only extras,
summary statistics, and optionally masks PII columns in reports.

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core API](#core-api)
  - [compare_dataframes()](#compare_dataframes)
  - [ValidationResult](#validationresult)
- [Reporting Utilities](#reporting-utilities)
  - [build_mismatch_report()](#build_mismatch_report)
  - [get_schema_diff()](#get_schema_diff)
  - [get_row_counts()](#get_row_counts)
  - [get_duplicate_report()](#get_duplicate_report)
  - [get_null_analysis()](#get_null_analysis)
- [Data Cleaning](#data-cleaning)
  - [trim_whitespace()](#trim_whitespace)
  - [clean_control_characters()](#clean_control_characters)
- [PII Masking](#pii-masking)
  - [mask_pii_columns()](#mask_pii_columns)
  - [Masking Strategies](#masking-strategies)
- [DataFrame Utilities](#dataframe-utilities)
  - [normalize_columns()](#normalize_columns)
  - [cast_all_to_string()](#cast_all_to_string)
- [PySpark Support](#pyspark-support)
- [Report Columns Modes](#report-columns-modes)
- [Full Example](#full-example)
- [Development](#development)
- [License](#license)

---

## Features

- **Hash-based comparison engine** — Efficiently compares DataFrames using SHA-256 row hashing with a two-stage algorithm
- **Dual DataFrame support** — Every function works with both **pandas** and **PySpark** DataFrames
- **Structured results** — Returns typed `ValidationResult` dataclass, not raw DataFrames
- **Mismatch detail** — Per-column diff strings: `[{col : (src_val:tgt_val)}, ...]`
- **PII masking** — 4 built-in strategies: alternate, hash, redact, partial
- **Schema comparison** — Detect missing, extra, and type-mismatched columns
- **Data cleaning** — Trim whitespace, remove control characters
- **Null analysis** — Report null counts per column
- **Duplicate detection** — Find duplicate rows by key columns
- **Zero mutation** — Input DataFrames are never modified
- **Minimal dependencies** — Only `pandas` required; PySpark is optional

---

## Installation

**Requires Python 3.9 or higher.** Tested on Python 3.9, 3.10, 3.11, and 3.12.

PySpark support requires **PySpark 3.3.0 or higher** (optional). Tested with PySpark 3.5.
Note: PySpark 3.3–3.5 supports Python 3.9–3.11; PySpark 4.0+ adds Python 3.12 support.

### Install

```bash
pip install databridge-validator
```

This installs the library with pandas support. It also works with PySpark DataFrames if PySpark is already installed in your environment (e.g., Databricks, EMR, or local Spark setup).

If you need PySpark installed alongside the library:

```bash
pip install databridge-validator[spark]
```

---

## Quick Start

```python
import pandas as pd
from databridge_validator import compare_dataframes

# Source data (legacy system)
source = pd.DataFrame({
    "id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Charlie", "Dave"],
    "amount": [100.0, 200.0, 300.0, 400.0],
})

# Target data (cloud migration)
target = pd.DataFrame({
    "id": [1, 2, 3, 5],
    "name": ["Alice", "Bobby", "Charlie", "Eve"],
    "amount": [100.0, 250.0, 300.0, 500.0],
})

result = compare_dataframes(source, target, key_columns=["id"])

print(result)
# ValidationResult:
#   is_match         = False
#   total_source     = 4
#   total_target     = 4
#   matched_count    = 2
#   mismatch_count   = 1
#   source_extra     = 1
#   target_extra     = 1

# Mismatched rows (key matches, values differ)
print(result.mismatch_records[["id", "mismatch_columns"]])
#    id                                    mismatch_columns
# 0   2  [{name : (Bob:Bobby)}, {amount : (200.0:250.0)}]

# Rows only in source
print(result.source_extra_records)
#    id  name  amount
# 0   4  Dave   400.0

# Rows only in target
print(result.target_extra_records)
#    id name  amount
# 0   5  Eve   500.0
```

---

## Core API

### `compare_dataframes()`

The main entry point. Compares source and target DataFrames and returns a structured `ValidationResult`.

```python
from databridge_validator import compare_dataframes

result = compare_dataframes(
    source_df,
    target_df,
    key_columns=["id"],
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source_df` | `DataFrame` | *(required)* | Source/legacy DataFrame (pandas or PySpark) |
| `target_df` | `DataFrame` | *(required)* | Target/cloud DataFrame (pandas or PySpark) |
| `key_columns` | `List[str]` | *(required)* | Columns to use as join keys for matching rows |
| `exclude_columns` | `List[str]` | `None` | Columns to skip during value comparison |
| `pii_columns` | `List[str]` | `None` | Columns containing PII to mask in reports |
| `mask_strategy` | `str` | `"alternate"` | Masking strategy: `"alternate"`, `"hash"`, `"redact"`, `"partial"` |
| `report_columns` | `str` | `"target"` | Which data columns in the mismatch report: `"target"`, `"source"`, `"both"` |
| `num_partitions` | `int` | `None` | Spark repartition count (PySpark only) |
| `persist` | `bool` | `False` | Persist intermediate Spark DataFrames (PySpark only) |
| `case_sensitive` | `bool` | `False` | Case-sensitive column name matching |

**Returns:** [`ValidationResult`](#validationresult)

**Advanced usage:**

```python
result = compare_dataframes(
    source_df=source,
    target_df=target,
    key_columns=["id", "date"],
    exclude_columns=["updated_at"],
    pii_columns=["ssn", "email"],
    mask_strategy="partial",
    report_columns="both",
    case_sensitive=False,
)
```

---

### `ValidationResult`

Structured dataclass returned by `compare_dataframes()`.

```python
from databridge_validator import ValidationResult
```

**Fields:**

| Field | Type | Description |
|---|---|---|
| `is_match` | `bool` | `True` if source and target are identical |
| `total_source_rows` | `int` | Row count in source DataFrame |
| `total_target_rows` | `int` | Row count in target DataFrame |
| `matched_count` | `int` | Rows matching by key and value |
| `mismatch_count` | `int` | Rows matching by key but differing in values |
| `source_extra_count` | `int` | Rows in source but not target |
| `target_extra_count` | `int` | Rows in target but not source |
| `mismatch_records` | `DataFrame or None` | DataFrame of mismatched rows with `mismatch_columns` |
| `source_extra_records` | `DataFrame or None` | DataFrame of source-only rows |
| `target_extra_records` | `DataFrame or None` | DataFrame of target-only rows |
| `summary` | `Dict[str, Any]` | Summary statistics dictionary |
| `metadata` | `Dict[str, Any]` | Additional metadata about the comparison |

**Methods:**

```python
# Convert to dictionary (excludes DataFrame fields)
result_dict = result.to_dict()

# Human-readable summary
print(result)
```

---

## Reporting Utilities

### `build_mismatch_report()`

Build a detailed mismatch report for rows that exist in both DataFrames (by key) but differ in values.

```python
from databridge_validator import build_mismatch_report

report = build_mismatch_report(
    source_df=source,
    target_df=target,
    key_columns=["id"],
    exclude_columns=["updated_at"],  # optional
    report_columns="target",          # optional: "target", "source", or "both"
)

print(report[["id", "mismatch_columns"]])
#    id                                    mismatch_columns
# 0   2  [{name : (Bob:Bobby)}, {amount : (200.0:250.0)}]
```

The `mismatch_columns` column contains a string listing every column that differs for that row:
```
[{col1 : (source_value:target_value)}, {col2 : (source_value:target_value)}]
```

---

### `get_schema_diff()`

Compare schemas between two DataFrames. Detects missing columns, extra columns, and type mismatches.

```python
from databridge_validator import get_schema_diff

diff = get_schema_diff(source_df, target_df)

print(diff.source_only_columns)   # Columns only in source
print(diff.target_only_columns)   # Columns only in target
print(diff.common_columns)        # Columns in both
print(diff.type_mismatches)       # {"col": ("int64", "float64")}
print(diff.is_compatible)         # True if schemas match
```

Returns a `SchemaDiff` dataclass:

| Field | Type | Description |
|---|---|---|
| `source_only_columns` | `List[str]` | Columns in source but not target |
| `target_only_columns` | `List[str]` | Columns in target but not source |
| `common_columns` | `List[str]` | Columns in both DataFrames |
| `type_mismatches` | `Dict[str, tuple]` | Column → (source_type, target_type) |
| `is_compatible` | `bool` | Property: `True` if no differences |

---

### `get_row_counts()`

Quick row count comparison between two DataFrames.

```python
from databridge_validator import get_row_counts

counts = get_row_counts(source_df, target_df)
print(counts)
# {
#     "source_count": 1000,
#     "target_count": 998,
#     "difference": 2,
#     "is_count_match": False,
# }
```

---

### `get_duplicate_report()`

Find duplicate rows based on key columns.

```python
from databridge_validator import get_duplicate_report

duplicates = get_duplicate_report(df, key_columns=["id"])
print(f"Found {len(duplicates)} duplicate rows")
print(duplicates)
```

Returns a DataFrame containing only rows with duplicate keys.

---

### `get_null_analysis()`

Analyze null/empty counts per column in a DataFrame.

```python
from databridge_validator import get_null_analysis

analysis = get_null_analysis(df)
print(analysis)
# {
#     "id":    {"null_count": 0, "total_count": 100},
#     "name":  {"null_count": 5, "total_count": 100},
#     "email": {"null_count": 12, "total_count": 100},
# }
```

---

## Data Cleaning

### `trim_whitespace()`

Trim leading and trailing whitespace from all string columns.

```python
from databridge_validator import trim_whitespace

cleaned = trim_whitespace(df)
# "  Alice  " → "Alice"
```

> Input DataFrame is never mutated. Returns a new DataFrame.

---

### `clean_control_characters()`

Remove control characters (`\r`, `\n`, `\t`, `\x00`, etc.) from all string columns.

```python
from databridge_validator import clean_control_characters

# Use default pattern (removes \r, \n, \t, \x00, \x07, \x08, \x0b, \x0c, \x1b, \u00a0, \u200b)
cleaned = clean_control_characters(df)

# Or supply a custom regex pattern
cleaned = clean_control_characters(df, pattern=r"[\r\n\t]")
```

> Input DataFrame is never mutated. Returns a new DataFrame.

---

## PII Masking

### `mask_pii_columns()`

Apply PII masking to specified columns in a DataFrame.

```python
from databridge_validator import mask_pii_columns

masked = mask_pii_columns(
    df,
    pii_columns=["ssn", "email"],
    mask_strategy="alternate",
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pii_columns` | `List[str]` | *(required)* | Column names containing PII |
| `mask_strategy` | `str` | `"alternate"` | One of: `"alternate"`, `"hash"`, `"redact"`, `"partial"` |

> Input DataFrame is never mutated. Returns a new DataFrame.

When used with `compare_dataframes()`, PII masking is applied automatically to **all three output DataFrames** (mismatch records, source extras, target extras) including inside the `mismatch_columns` string:

```python
result = compare_dataframes(
    source, target,
    key_columns=["id"],
    pii_columns=["ssn", "email"],
    mask_strategy="alternate",
)
# SSN in mismatch_columns: [{ssn : (2*2*2*-*2*2:9*9*9*-*9*9)}]
```

---

### Masking Strategies

| Strategy | Function | Example | Description |
|---|---|---|---|
| `"alternate"` | `mask_alternate_chars()` | `"Hello"` → `"H*l*o"` | Mask every other character |
| `"hash"` | `mask_with_hash()` | `"Hello"` → `"185f8db3..."` | SHA-256 hash of the value |
| `"redact"` | `mask_redact()` | `"Hello"` → `"***"` | Replace entirely with placeholder |
| `"partial"` | `mask_partial()` | `"Hello"` → `"H***o"` | Show first/last N characters |

Each strategy function can also be called directly:

```python
from databridge_validator import mask_alternate_chars, mask_with_hash, mask_redact, mask_partial

mask_alternate_chars("Hello")       # "H*l*o"
mask_with_hash("Hello")             # "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969"
mask_redact("Hello")                # "***"
mask_partial("Hello", visible_chars=1)  # "H***o"
```

---

## DataFrame Utilities

### `normalize_columns()`

Normalize column names: lowercase, strip whitespace, replace spaces with underscores.

```python
from databridge_validator import normalize_columns

# Before: ["First Name", " AGE ", "Email Address"]
normalized = normalize_columns(df)
# After:  ["first_name", "age", "email_address"]
```

---

### `cast_all_to_string()`

Cast all columns to string type. Useful for standardizing types before comparison.

```python
from databridge_validator import cast_all_to_string

string_df = cast_all_to_string(df)
```

> Null/NaN values are preserved (not converted to the string `"None"`).

---

## PySpark Support

All functions work with PySpark DataFrames. PySpark is an **optional dependency** — the library
works with pandas alone.

```bash
pip install databridge-validator[spark]
```

```python
from pyspark.sql import SparkSession
from databridge_validator import compare_dataframes

spark = SparkSession.builder.appName("validation").getOrCreate()

source_df = spark.createDataFrame(source_data)
target_df = spark.createDataFrame(target_data)

result = compare_dataframes(
    source_df,
    target_df,
    key_columns=["id"],
    num_partitions=200,      # Spark-specific: repartition for parallelism
    persist=True,            # Spark-specific: cache intermediate results
)
```

**Spark-specific parameters:**

| Parameter | Description |
|---|---|
| `num_partitions` | Repartition DataFrames before comparison (improves parallelism) |
| `persist` | Cache intermediate DataFrames in `MEMORY_AND_DISK` storage level |

**Algorithm:** The Spark implementation uses an optimized two-stage approach:
1. **Stage 1:** Hash all columns (keys + compare columns) with SHA-256. Perform hash anti-join to eliminate all matching rows — this produces a small delta set.
2. **Stage 2:** Key-based anti-joins and inner-joins on the small delta sets to separate extras from mismatches.

This is efficient because most rows typically match, so Stage 1 eliminates the majority of data before the more expensive key joins.

---

## Report Columns Modes

The `report_columns` parameter controls which data columns appear in the mismatch report:

### `"target"` (default)
Key columns + target DataFrame column values:
```python
result = compare_dataframes(source, target, key_columns=["id"], report_columns="target")
# Mismatch report columns: id, name, amount, mismatch_columns
# Values from target DataFrame
```

### `"source"`
Key columns + source DataFrame column values:
```python
result = compare_dataframes(source, target, key_columns=["id"], report_columns="source")
# Mismatch report columns: id, name, amount, mismatch_columns
# Values from source DataFrame
```

### `"both"`
Key columns + columns from both DataFrames (suffixed with `_source` / `_target`):
```python
result = compare_dataframes(source, target, key_columns=["id"], report_columns="both")
# Mismatch report columns: id, name_source, name_target, amount_source, amount_target, mismatch_columns
```

---

## Full Example

```python
import pandas as pd
from databridge_validator import (
    compare_dataframes,
    get_schema_diff,
    get_row_counts,
    get_null_analysis,
    trim_whitespace,
    clean_control_characters,
)

# 1. Load your DataFrames (from any source — DB, CSV, Parquet, etc.)
source = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["  Alice  ", "Bob", "Charlie", "Dave", "Eve"],
    "email": ["a@test.com", "b@test.com", "c@test.com", "d@test.com", "e@test.com"],
    "ssn": ["111-11-1111", "222-22-2222", "333-33-3333", "444-44-4444", "555-55-5555"],
    "amount": [100.0, 200.0, 300.0, 400.0, 500.0],
})

target = pd.DataFrame({
    "id": [1, 2, 3, 6, 7],
    "name": ["Alice", "Bobby", "Charlie", "Frank", "Grace"],
    "email": ["a@test.com", "b_new@test.com", "c@test.com", "f@test.com", "g@test.com"],
    "ssn": ["111-11-1111", "999-99-9999", "333-33-3333", "666-66-6666", "777-77-7777"],
    "amount": [100.0, 250.0, 300.0, 600.0, 700.0],
})

# 2. Clean data before comparison
source = trim_whitespace(source)
source = clean_control_characters(source)

# 3. Pre-flight checks
print(get_row_counts(source, target))
print(get_schema_diff(source, target).is_compatible)
print(get_null_analysis(source))

# 4. Run comparison with PII masking
result = compare_dataframes(
    source_df=source,
    target_df=target,
    key_columns=["id"],
    pii_columns=["ssn", "email"],
    mask_strategy="alternate",
    report_columns="target",
)

# 5. Inspect results
print(result)
print(result.summary)

if not result.is_match:
    if result.mismatch_records is not None:
        print("\n--- Mismatched Rows ---")
        print(result.mismatch_records.to_string(index=False))

    if result.source_extra_records is not None:
        print("\n--- Source-Only Rows ---")
        print(result.source_extra_records.to_string(index=False))

    if result.target_extra_records is not None:
        print("\n--- Target-Only Rows ---")
        print(result.target_extra_records.to_string(index=False))

# 6. Export summary as dict
report_dict = result.to_dict()
```

---

## Development

For contributors working on the library itself.

### Setup

```bash
git clone https://github.com/databridge-validator/databridge-validator.git
cd databridge-validator

# Install with all dev tools (pytest, ruff, mypy, etc.)
pip install -e ".[dev]"

# Install with dev tools + PySpark (for running Spark integration tests)
pip install -e ".[all]"
```

### Run tests

```bash
# Unit tests with coverage
pytest tests/unit/ --cov=databridge_validator --cov-fail-under=80 -v

# All tests including Spark integration (requires PySpark)
pytest --cov=databridge_validator --cov-fail-under=80 -v
```

### Lint and format

```bash
ruff check src/ tests/
ruff format --check src/ tests/
```

### Build package

```bash
python -m build
twine check dist/*
```

---

## License

MIT