"""This file configures pytest."""
import sys
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

# Add src/ to Python path so tests can import from it
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a SparkSession fixture for tests."""
    return (
        SparkSession.builder
        .appName("test")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture(scope="session", autouse=True)
def cleanup_spark(spark):
    """Cleanup Spark session after all tests."""
    yield
    spark.stop()
