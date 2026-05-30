from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

MOVIES_SCHEMA = StructType(
    [
        StructField("movieId", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("genres", StringType(), True),
    ]
)

RATINGS_SCHEMA = StructType(
    [
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("rating", DoubleType(), False),
        StructField("timestamp", LongType(), False),
    ]
)

TAGS_SCHEMA = StructType(
    [
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("tag", StringType(), True),
        StructField("timestamp", StringType(), False),
    ]
)

LINKS_SCHEMA = StructType(
    [
        StructField("movieId", IntegerType(), False),
        StructField("imdbId", StringType(), True),
        StructField("tmdbId", StringType(), True),
    ]
)

GENOME_SCORES_SCHEMA = StructType(
    [
        StructField("movieId", IntegerType(), False),
        StructField("tagId", IntegerType(), False),
        StructField("relevance", DoubleType(), False),
    ]
)

GENOME_TAGS_SCHEMA = StructType(
    [
        StructField("tagId", IntegerType(), False),
        StructField("tag", StringType(), False),
    ]
)

SCHEMAS = {
    "movies": MOVIES_SCHEMA,
    "ratings": RATINGS_SCHEMA,
    "tags": TAGS_SCHEMA,
    "links": LINKS_SCHEMA,
    "genome_scores": GENOME_SCORES_SCHEMA,
    "genome_tags": GENOME_TAGS_SCHEMA,
}

FILENAMES = {
    "movies": "movie.csv",
    "ratings": "rating.csv",
    "tags": "tag.csv",
    "links": "link.csv",
    "genome_scores": "genome_scores.csv",
    "genome_tags": "genome_tags.csv",
}


def dataset_names():
    return list(SCHEMAS.keys())


def get_schema(dataset):
    return SCHEMAS[dataset]


def get_filename(dataset):
    return FILENAMES[dataset]
