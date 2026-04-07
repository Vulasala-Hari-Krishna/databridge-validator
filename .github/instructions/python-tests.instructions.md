---
applyTo: "tests/**/*.py"
---

# Test Code Instructions — databridge-validator

## Test Organization

- `tests/unit/` — Fast, hermetic, no external IO. These run on every commit.
- `tests/integration/` — Larger DataFrames, real Spark sessions (optional). Run in CI.
- `tests/conftest.py` — Shared fixtures only. No test functions here.

## Naming Convention

```
test_<function_name>_<scenario>_<expected_behavior>
```

Examples:
- `test_compare_dataframes_identical_data_returns_zero_mismatches`
- `test_compare_dataframes_empty_source_returns_all_target_as_extras`
- `test_mask_alternate_chars_with_none_input_returns_none`
- `test_clean_control_characters_removes_newlines_and_tabs`
- `test_compare_dataframes_mismatched_types_raises_type_error`

## Fixture Design (conftest.py)

```python
import pytest
import pandas as pd

@pytest.fixture
def sample_source_df():
    """Standard source DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "email": ["alice@test.com", "bob@test.com", "charlie@test.com", "diana@test.com", "eve@test.com"],
        "amount": [100.0, 200.0, 300.0, 400.0, 500.0],
    })

@pytest.fixture
def sample_target_df():
    """Standard target DataFrame with intentional differences."""
    return pd.DataFrame({
        "id": [1, 2, 3, 6, 7],
        "name": ["Alice", "Bobby", "Charlie", "Frank", "Grace"],
        "email": ["alice@test.com", "bob_new@test.com", "charlie@test.com", "frank@test.com", "grace@test.com"],
        "amount": [100.0, 250.0, 300.0, 600.0, 700.0],
    })
    # Expected: id=1 match, id=2 mismatch, id=3 match, id=4,5 source extras, id=6,7 target extras
```

## Edge Cases MUST Be Tested

Every comparison/validation function needs tests for:

| Edge Case | Why It Matters |
|---|---|
| Empty DataFrames (0 rows) | Should not crash; return empty results |
| Single-row DataFrames | Boundary condition |
| All rows match perfectly | `is_match` should be True |
| All rows mismatch | Worst case performance |
| Null/None values in key columns | Join behavior with nulls |
| Null/None values in data columns | Comparison logic with nulls |
| Whitespace-only strings | Should be treated as empty/null |
| Duplicate keys in source or target | Many-to-many join handling |
| Column name case differences | Case-insensitive mode |
| Extra columns in source or target | Should handle gracefully |
| Type mismatches (int vs string) | Casting behavior |
| Special characters in data | Unicode, control chars |
| Very long strings | No truncation issues |
| DataFrames with different column orders | Should still compare correctly |

## PII Masking Tests

```python
class TestMaskAlternateChars:
    def test_basic_string(self):
        assert mask_alternate_chars("Hello") == "H*l*o"

    def test_empty_string(self):
        assert mask_alternate_chars("") == ""

    def test_none_input(self):
        assert mask_alternate_chars(None) is None

    def test_single_char(self):
        assert mask_alternate_chars("A") == "A"

    def test_numeric_string(self):
        assert mask_alternate_chars("123456") == "1*3*5*"
```

## Spark Test Handling

```python
import pytest

# Skip Spark tests if PySpark not installed
spark_available = pytest.importorskip("pyspark", reason="PySpark not installed")

@pytest.fixture(scope="session")
def spark_session():
    """Create a local Spark session for integration tests."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("databridge-validator-tests") \
        .getOrCreate()
    yield spark
    spark.stop()
```

## Parametrize for Dual DataFrame Support

```python
@pytest.mark.parametrize("df_type", ["pandas", "spark"])
def test_compare_dataframes_with_mismatches(df_type, sample_source_df, spark_session):
    if df_type == "spark":
        pytest.importorskip("pyspark")
        source = spark_session.createDataFrame(sample_source_df)
        target = spark_session.createDataFrame(sample_target_df)
    else:
        source = sample_source_df
        target = sample_target_df
    result = compare_dataframes(source, target, key_columns=["id"])
    assert result.mismatch_count > 0
```

## Rules

- No `print()` — assertions only
- No file IO in unit tests
- No sleep/time-based tests
- No network calls
- Tests must be independent — no order dependency
- Use `pytest.raises(ExceptionType, match="pattern")` for error tests
- Target: ≥80% coverage on every file