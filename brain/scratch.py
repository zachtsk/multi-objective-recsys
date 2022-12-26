# Spark for initial data transformations
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

from brain.config import Config, LocalConfig, GoogleCloudConfig

# Create a SparkSession
spark = SparkSession.builder.appName("otto").getOrCreate()


@F.udf(
    StructType(
        [
            StructField("aid", IntegerType()),
            StructField("ts", LongType()),
            StructField("event", StringType()),
        ]
    )
)
def unpack_events(col):
    """Expect a column with format (aid, ts, event)"""
    return col[0], col[1], col[2]


def make_explode_events(df):
    """
    Explodes the event records, so each session has multiple event rows,
    and unpacks the variables from each event.

    INPUT:
    +--------------------+--------+
    |              events| session|
    +--------------------+--------+
    |[{59625, 16617240...|12899779|
    |[{1142000, 166172...|12899780|
    |[{141736, 1661724...|12899781|
    |[{1669402, 166172...|12899782|
    +--------------------+--------+

    OUTPUT:
    +--------+-------+-------------+------+
    |session |aid    |ts           |event |
    +--------+-------+-------------+------+
    |12899780|1142000|1661724000378|clicks|
    |12899780|582732 |1661724058352|clicks|
    |12899780|973453 |1661724109199|clicks|
    |12899780|736515 |1661724136868|clicks|
    +--------+-------+-------------+------+
    """
    return (
        df.withColumn("events", F.explode("events"))
        .withColumn("events", unpack_events(F.col("events")))
        .select(
            "session",
            F.col("events.aid").alias("aid"),
            F.col("events.ts").alias("ts"),
            F.col("events.event").alias("event"),
        )
    )


def make_next_aid(df):
    """
    Retrieve the next "aid" that the user took action on

    INPUT:
    +--------+-------+-------------+------+
    |session |aid    |ts           |event |
    +--------+-------+-------------+------+
    |12899780|1142000|1661724000378|clicks|
    |12899780|582732 |1661724058352|clicks|
    |12899780|973453 |1661724109199|clicks|
    |12899780|736515 |1661724136868|clicks|
    +--------+-------+-------------+------+

    OUTPUT:
    +--------+-------+-------------+------+--------+
    |session |aid    |ts           |event |next_aid|
    +--------+-------+-------------+------+--------+
    |12899780|1142000|1661724000378|clicks|582732  |
    |12899780|582732 |1661724058352|clicks|973453  |
    |12899780|973453 |1661724109199|clicks|736515  |
    |12899780|736515 |1661724136868|clicks|1142000 |
    +--------+-------+-------------+------+--------+
    """
    # Partition by `user`, sort by descending `ts`
    partition = W.partitionBy("session").orderBy(F.desc("ts"))
    return (
        df
        # Get the next aid for a given session, using the partition window
        .withColumn("next_aid", F.lag("aid").over(partition))
        # Update datatype
        .withColumn("next_aid", F.expr(f"cast(next_aid as integer)"))
        # The last row for each session has a NULL "next_aid"
        .where("next_aid IS NOT NULL")
    )


def normalize_data(config: Config, train_test: str, sample_size: int = None):
    # I/O Paths
    if train_test == "train":
        in_fp = str(config.train_jsonl_fp)
        out_fp = str(config.train_fp)
    else:
        in_fp = str(config.test_jsonl_fp)
        out_fp = str(config.test_fp)

    # Read file
    df = spark.read.json(in_fp, multiLine=False, lineSep="\n")

    # Determine filters and output
    if sample_size:
        df = df.limit(sample_size)

    # Normalize operation
    df = make_explode_events(df)
    df.write.parquet(out_fp, mode="overwrite")


def main():
    """
    Command line tool for running data engineering jobs
    """
    # Init parser
    parser = argparse.ArgumentParser()

    # Ordinal argument for the function to select
    # OPTIONS: ['create', 'upload']
    parser.add_argument("function")

    # Parameters
    parser.add_argument("-c", "--config")
    parser.add_argument("-t", "--train_test")
    parser.add_argument("-s", "--sample_size")

    # Parse arguments
    args = parser.parse_args()

    # Dispatch functions
    if args.function == "normalize":
        # Use local or cloud config
        if args.config == "google":
            config = GoogleCloudConfig()
        else:
            config = LocalConfig()

        # Determine Sample Size
        if args.sample_size == "sm":
            sample_size = config.size_sm
        elif args.sample_size == "med":
            sample_size = config.size_med
        elif args.sample_size == "lg":
            sample_size = config.size_lg
        else:
            sample_size = None

        normalize_data(config, args.train_test, sample_size)


if __name__ == "__main__":
    main()
