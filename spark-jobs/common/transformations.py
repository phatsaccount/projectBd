from pyspark.sql import functions as F


def trim_string_columns(df, columns):
    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, F.trim(F.col(column)))
    return df


def empty_to_null(df, columns):
    for column in columns:
        if column in df.columns:
            df = df.withColumn(
                column,
                F.when(F.trim(F.col(column)) == "", None).otherwise(F.col(column)),
            )
    return df


def normalize_tag_column(df, column):
    if column not in df.columns:
        return df
    return df.withColumn(column, F.lower(F.col(column)))


def normalize_genres(df, source_col, target_col):
    if source_col not in df.columns:
        return df

    normalized = F.lower(F.col(source_col))
    items = F.split(normalized, r"\|")
    items = F.transform(items, lambda x: F.trim(x))
    items = F.filter(items, lambda x: x != "")
    items = F.array_distinct(items)
    items = F.sort_array(items)

    normalized_col = (
        F.when(
            F.col(source_col).isNull()
            | (F.trim(F.col(source_col)) == "")
            | (F.lower(F.col(source_col)) == "(no genres listed)"),
            None,
        )
        .when(F.size(items) == 0, None)
        .otherwise(F.array_join(items, "|"))
    )

    return df.withColumn(target_col, normalized_col)


def genres_to_array(df, source_col, target_col):
    if source_col not in df.columns:
        return df

    items = F.split(F.col(source_col), r"\|")
    items = F.filter(items, lambda x: x != "")
    empty_array = F.expr("array()")

    return df.withColumn(
        target_col, F.when(F.col(source_col).isNull(), empty_array).otherwise(items)
    )
