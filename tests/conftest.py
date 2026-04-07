"""Shared test fixtures for databridge-validator."""

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
    """Standard target DataFrame with intentional differences.

    Expected results when joined on 'id':
    - id=1: match (same name, email, amount)
    - id=2: mismatch (name, email, amount differ)
    - id=3: match (same name, email, amount)
    - id=4, 5: source extras (not in target)
    - id=6, 7: target extras (not in source)
    """
    return pd.DataFrame({
        "id": [1, 2, 3, 6, 7],
        "name": ["Alice", "Bobby", "Charlie", "Frank", "Grace"],
        "email": ["alice@test.com", "bob_new@test.com", "charlie@test.com", "frank@test.com", "grace@test.com"],
        "amount": [100.0, 250.0, 300.0, 600.0, 700.0],
    })


@pytest.fixture
def empty_df():
    """Empty DataFrame with standard columns."""
    return pd.DataFrame({"id": pd.Series([], dtype="int64"), "name": pd.Series([], dtype="str")})


@pytest.fixture
def single_row_df():
    """Single-row DataFrame."""
    return pd.DataFrame({"id": [1], "name": ["Alice"], "amount": [100.0]})


@pytest.fixture
def df_with_nulls():
    """DataFrame containing null values."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", None, "Charlie"],
        "email": [None, "bob@test.com", None],
    })


@pytest.fixture
def df_with_whitespace():
    """DataFrame with leading/trailing whitespace in string columns."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["  Alice  ", "Bob  ", "  Charlie"],
        "email": [" alice@test.com ", "bob@test.com", "  charlie@test.com  "],
    })


@pytest.fixture
def df_with_control_chars():
    """DataFrame with control characters in string columns."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice\r\n", "Bob\t", "Charlie\x00"],
        "email": ["alice@test.com\n", "bob@test.com\r", "charlie@test.com"],
    })
