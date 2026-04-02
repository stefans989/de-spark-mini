from pyspark.sql import DataFrame

from src.contracts.users_contract import DataContract


def validate(df: DataFrame, contract: DataContract) -> None:
    """Validate a DataFrame against a DataContract.

    Raises ValueError listing missing required columns and, in strict mode,
    any extra columns not declared in the contract.
    """
    df_columns = set(df.columns)
    required = set(contract.required_columns)

    missing = sorted(required - df_columns)
    extra = sorted(df_columns - required) if contract.strict else []

    errors = []
    if missing:
        errors.append(f"Missing required columns: {missing}")
    if extra:
        errors.append(f"Extra columns not allowed in strict mode: {extra}")

    if errors:
        raise ValueError(" | ".join(errors))
