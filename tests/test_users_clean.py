from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.jobs.users_clean import transform_users, add_quality_flags


def spark():
    import os, sys

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


def test_transform_and_quality_flags():
    s = spark()
    try:
        df = s.createDataFrame(
            [
                ("1", " ANA@EXAMPLE.COM ", "rs", "2026-03-30"),
                ("2", "invalid-email", "US", "2026-03-31"),
                (None, "x@example.com", "RS", "2026-04-01"),
                ("2", "dup@example.com", "DE", "2026-03-30"),
            ],
            ["user_id", "email", "country", "signup_date"],
        )

        clean = transform_users(df)
        # After dedupe by user_id: should have 3 rows (user_id 1, 2, null)
        assert clean.count() == 3

        flagged = add_quality_flags(clean)

        # user_id=1 should be good
        good_1 = flagged.filter(F.col("user_id") == 1).select("_is_bad").collect()[0][0]
        assert good_1 is False

        # user_id=2 has invalid email -> bad
        bad_2 = flagged.filter(F.col("user_id") == 2).select("_is_bad").collect()[0][0]
        assert bad_2 is True

        # null user_id -> bad
        bad_null = flagged.filter(F.col("user_id").isNull()).select("_is_bad").collect()[0][0]
        assert bad_null is True

    finally:
        s.stop()
