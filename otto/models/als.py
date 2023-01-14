from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import functions as F, SparkSession
from otto.config import Config


def fit_als_model(config: Config):
    spark = SparkSession.builder.appName("als_model").getOrCreate()

    # Create model training set
    train = spark.read.parquet(config.train_fp)
    test = spark.read.parquet(config.test_fp)
    df = train.unionAll(test)

    # Sample from available sessions for training
    train_sessions = (
        df.select("session")
        .distinct()
        .sample(fraction=config.als_train_sample, seed=100)
    )

    # Create implicit ratings dataset
    als_train = (
        df.join(train_sessions, on=["session"])
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
        fit_als_model(config)
        als = ALSModel.load(config.als_model_fp)
    return als
