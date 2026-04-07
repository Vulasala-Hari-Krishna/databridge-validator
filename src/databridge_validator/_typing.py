"""Internal type aliases for databridge_validator."""

from typing import Any, Union

import pandas as pd

# Use Any as a stand-in for pyspark.sql.DataFrame to avoid import errors
# when PySpark is not installed.
DataFrameType = Union[pd.DataFrame, Any]
