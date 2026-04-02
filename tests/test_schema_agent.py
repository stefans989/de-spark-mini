import os
import sys

import pytest
from pyspark.sql import SparkSession

from src.agents.schema_agent import validate
from src.contracts.users_contract import users_contract


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = SparkSession.builder.master("local[1]").appName("pytest-schema").getOrCreate()
    yield session
    session.stop()


def test_missing_required_column_raises(spark):
    # Drop 'email' so the contract required column is absent
    df = spark.createDataFrame(
        [("1", "RS", "2026-03-30")],
        ["user_id", "country", "signup_date"],
    )

    with pytest.raises(ValueError, match="Missing required columns"):
        validate(df, users_contract)


def test_extra_column_raises_in_strict_mode(spark):
    # All required columns present but an extra 'phone' column is added
    df = spark.createDataFrame(
        [("1", "a@b.com", "RS", "2026-03-30", "123456789")],
        ["user_id", "email", "country", "signup_date", "phone"],
    )

    with pytest.raises(ValueError, match="Extra columns not allowed in strict mode"):
        validate(df, users_contract)





@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()
