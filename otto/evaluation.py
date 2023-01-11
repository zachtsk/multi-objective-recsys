from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window as W

from otto.config import Config


def create_evaluation_set(config: Config):
    """
    Create evaluation dataset

    +--------+----------+-------+-------------+----------+
    |session |event_type|aid    |ts           |set       |
    +--------+----------+-------+-------------+----------+
    |12899781|clicks    |918667 |1662060160406|evaluation|
    |12899786|carts     |955252 |1661724017764|evaluation|
    |12899788|clicks    |1663048|1661724078816|evaluation|
    |12899794|clicks    |1242729|1661724312764|evaluation|
    +--------+----------+-------+-------------+----------+
    """
    # Create a SparkSession
    spark = SparkSession.builder.appName("otto").getOrCreate()

    # Import test dataframe
    test = spark.read.parquet(config.test_fp).withColumn("set", F.lit("test"))

    # Create Evaluation dataset:
    # - Latest click
    # - Latest 20 cart events
    # - Latest 20 order events
    latest_events = W.partitionBy("session", "event_type").orderBy(F.desc("ts"), F.rand())
    output = (
        test
        .withColumn("click_order", F.count("*").over(latest_events))
        .where("""
        (event_type='clicks' and click_order = 1) OR
        (event_type='carts' and click_order <= 20) OR
        (event_type='orders' and click_order <= 20)
        """)
        .select("session", "event_type", "aid", "ts", F.lit("evaluation").alias("set"))
    )
    output.write.parquet(config.eval_fp, mode="overwrite")


def evaluate(df_answers, df_test):
    return (
        df_answers
        .join(df_test.withColumn("hit", F.lit(1)), on=["session", "event_type", "aid"], how="left")
        .withColumn("hit", F.expr("greatest(hit,0)"))
        .agg(
            F.expr(
                f"sum(case when event_type='clicks' then hit else 0 end)/greatest(sum(case when event_type='clicks' then 1 else 0 end),1)").alias(
                "click_score"),
            F.expr(
                f"sum(case when event_type='carts' then hit else 0 end)/greatest(sum(case when event_type='carts' then 1 else 0 end),1)").alias(
                "cart_score"),
            F.expr(
                f"sum(case when event_type='orders' then hit else 0 end)/greatest(sum(case when event_type='orders' then 1 else 0 end),1)").alias(
                "order_score"),
        )
        .withColumn("final", F.expr("(click_score * .1) + (cart_score * .3) + (order_score * .6)"))
    )
