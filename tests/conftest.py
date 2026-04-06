import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Add repository root to sys.path so `import src...` works
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="function")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("de-spark-mini-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.warehouse.dir", str(ROOT / ".spark-warehouse"))
        .getOrCreate()
    )

    yield spark

    # Normal stop only (no gateway.shutdown)
    spark.stop()

    # Give Windows a moment to release ports / processes (reduces flakiness)
    import time
    time.sleep(0.5)