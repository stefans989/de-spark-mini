from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


ALLOWED_COUNTRIES = {"RS", "DE", "US"}


def build_spark(app_name: str = "users_clean") -> SparkSession:
    import os, sys

    # Force Spark to use THIS Python interpreter (your venv)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


def transform_users(df):
    out = (
        df.withColumn("user_id", F.col("user_id").cast("int"))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .withColumn("signup_date", F.to_date(F.col("signup_date")))
    )

    # Dedupe by user_id
    out = out.dropDuplicates(["user_id"])
    return out


def add_quality_flags(df):
    return (
        df.withColumn("_bad_user_id", F.col("user_id").isNull())
        .withColumn("_bad_email", F.col("email").isNull() | (~F.col("email").contains("@")))
        .withColumn("_bad_country", F.col("country").isNull() | (~F.col("country").isin(list(ALLOWED_COUNTRIES))))
        .withColumn("_is_bad", F.col("_bad_user_id") | F.col("_bad_email") | F.col("_bad_country"))
    )


def run(input_csv: str, good_output: str, bad_output: str) -> None:
    spark = build_spark()

    try:
        df = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(input_csv)
        )

        clean = transform_users(df)
        flagged = add_quality_flags(clean)

        good = flagged.filter(~F.col("_is_bad")).drop(
            "_bad_user_id", "_bad_email", "_bad_country", "_is_bad"
        )

        bad = flagged.filter(F.col("_is_bad"))

        good.coalesce(1).write.mode("overwrite").parquet(good_output)
        bad.coalesce(1).write.mode("overwrite").parquet(bad_output)

        # Mini summary (driver side)
        good_count = good.count()
        bad_count = bad.count()
        print(f"GOOD rows: {good_count}")
        print(f"BAD  rows: {bad_count}")

    finally:
        # Helps reduce orphan processes on Windows
        spark.stop()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=str(Path("data") / "users.csv"))
    p.add_argument("--good-output", default=str(Path("out") / "users_clean.parquet"))
    p.add_argument("--bad-output", default=str(Path("out") / "users_bad.parquet"))
    args = p.parse_args()

    run(args.input, args.good_output, args.bad_output)


if __name__ == "__main__":
    main()
