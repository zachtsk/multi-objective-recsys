from pyspark.ml.recommendation import ALS, ALSModel

from otto.config import Config
from pyspark.sql import functions as F


def create_als_training(config: Config):
    # Create model training set
    train = config.spark.read.parquet(config.train_fp)
    test = config.spark.read.parquet(config.test_fp)
    df = train.unionAll(test)

    # Sample from available sessions for training
    als_train_sessions = (
        df.select("session")
        .distinct()
        .sample(fraction=config.als_train_sample, seed=100)
    )

    # Create implicit ratings dataset
    als_train = (
        df.join(als_train_sessions, on=["session"])
        .select("session", "aid")
        .distinct()
        .withColumn("viewed", F.lit(1))
    )

    # Collaborative Filtering Model
    als = ALS(
        userCol=config.als_user_col,
        itemCol=config.als_item_col,
        ratingCol=config.als_rating_col,
        implicitPrefs=config.als_implicit_preferences,
        rank=config.als_embeddings,
        seed=100,
    )
    als = als.fit(als_train)
    als.write().overwrite().save(config.als_model_fp)


def load_als_model(config: Config) -> ALSModel:
    try:
        als = ALSModel.load(config.als_model_fp)
    except FileNotFoundError:
        create_als_training(config)
        als = ALSModel.load(config.als_model_fp)
    return als

