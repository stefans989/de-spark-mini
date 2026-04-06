import pytest

from src.agents.schema_agent import validate
from src.contracts.users_contract import users_contract


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