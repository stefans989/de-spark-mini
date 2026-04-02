# de-spark-mini

A mini Spark + Python data engineering project.

## What the pipeline does

- Reads `data/users.csv`
- Standardizes columns:
  - `user_id` -> `int`
  - `email` -> `trim` + `lower`
  - `country` -> `trim` + `upper`
  - `signup_date` -> `date`
- Deduplicates by `user_id` using `dropDuplicates(["user_id"])`
- Splits data into:
  - **good** rows -> `out/users_clean.parquet`
  - **bad** rows (quarantine, includes quality flags) -> `out/users_bad.parquet`

A row is considered **bad** if any of these are true:
- `user_id` is null
- `email` is null or does not contain `@`
- `country` is null or not in the allowed set: `RS`, `DE`, `US`

## Setup (Windows)

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install pyspark pytest ruff
```

## Run

```powershell
python -m src.jobs.users_clean `
  --input data/users.csv `
  --good-output out/users_clean.parquet `
  --bad-output out/users_bad.parquet
```

## Test

```powershell
pytest -q
```