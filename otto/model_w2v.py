from pyspark.ml.feature import Word2Vec
from pyspark.sql import functions as F, SparkSession

from otto.config import Config


def fit_w2v_model(config: Config):
    spark = SparkSession.builder.appName("w2v_model").getOrCreate()

    # Create model training set
    train = spark.read.parquet(config.train_fp)
    test = spark.read.parquet(config.test_fp)
    df = train.unionAll(test)

    # Sample data
    train_sessions = (
        df.select("session")
        .distinct()
        .sample(fraction=config.w2v_train_sample, seed=100)
    )

    # Training data
    sentences = (
        df.join(train_sessions, on=["session"])
        .withColumn("aid", F.expr("cast(aid as string)"))
        .groupby("session")
        .agg(F.collect_list("aid").alias("aid_ls"))
    )

    # Word2Vec Model
    w2v_model = Word2Vec(
        vectorSize=config.w2v_embeddings,
        minCount=config.w2v_min_count,
        inputCol=config.w2v_sentence_col,
        windowSize=config.w2v_window_size,
        maxIter=config.w2v_max_iter,
    )
    model = w2v_model.fit(sentences)
    model.write().overwrite().save(config.w2v_model_fp)
