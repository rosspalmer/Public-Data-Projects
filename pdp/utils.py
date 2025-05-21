
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def parse_fixed_width(text_df: DataFrame, columns: list[tuple[str, int, str]], text_column: str = "value") -> DataFrame:

    select_columns = []
    start_pos = 1

    for name, width, data_type in columns:
        select_columns.append(F.col(text_column).substr(start_pos, width).cast(data_type).alias(name))
        start_pos += width

    other_columns = [F.col(c) for c in text_df.columns if c != text_column]

    df = text_df.select(
        other_columns + select_columns
    )

    return df
