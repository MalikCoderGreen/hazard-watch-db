from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def parse_damage_amount(df: DataFrame, column: str, output_column: str) -> DataFrame:
    """Parse NCEI magnitude-suffixed damage strings (e.g. '10.00K', '1.5M', '2B') into numeric dollars."""
    numeric_part = F.regexp_extract(F.col(column), r"^([0-9]*\.?[0-9]+)", 1).cast("double")
    suffix = F.upper(F.regexp_extract(F.col(column), r"([A-Za-z])$", 1))
    multiplier = (
        F.when(suffix == "K", F.lit(1_000.0))
        .when(suffix == "M", F.lit(1_000_000.0))
        .when(suffix == "B", F.lit(1_000_000_000.0))
        .otherwise(F.lit(1.0))
    )
    return df.withColumn(
        output_column,
        F.when((F.col(column).isNull()) | (F.col(column) == ""), F.lit(0.0)).otherwise(numeric_part * multiplier),
    )


def severity_score(df: DataFrame, deaths_col: str, injuries_col: str, damage_col: str, output_column: str) -> DataFrame:
    """Simple composite severity score for cross-source ranking in gold aggregations."""
    return df.withColumn(
        output_column,
        (F.coalesce(F.col(deaths_col), F.lit(0)) * 100)
        + (F.coalesce(F.col(injuries_col), F.lit(0)) * 10)
        + (F.coalesce(F.col(damage_col), F.lit(0.0)) / 1_000_000),
    )


def derive_state_abbr_from_ugc(ugc_column: str, output_column: str):
    """Return a column expression deriving a 2-letter state abbreviation from a UGC zone code (e.g. 'DCZ001' -> 'DC')."""
    return F.upper(F.substring(F.col(ugc_column), 1, 2)).alias(output_column)
