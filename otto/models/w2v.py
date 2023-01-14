from typing import List
import multiprocessing
from gensim.models import Word2Vec
from annoy import AnnoyIndex
from pyspark.sql import functions as F, SparkSession
from otto.config import Config


# Steps:
# 1. Extract all sentences
# 2. Train W2V Model
# 3. Save word embeddings
# 4. Index word embeddings to annoy index
# 5. Build Nearest Neighbors dataset


def materialize_sentences(config: Config, sample: int = None):
    """
    Return a list of lists of items viewed in each session.
    It also allows the user to sample the data if desired.

    Parameters:
        config (Config): A Config object containing the file paths of the train and test datasets.
        sample (int, optional): The number of sessions to sample. Defaults to None.

    Returns:
        list: A list of lists of items viewed in each session.
    """
    spark = SparkSession.builder.appName("w2v_model").getOrCreate()

    # Load datasets
    train = spark.read.parquet(config.train_fp)
    test = spark.read.parquet(config.test_fp)
    df = test.unionAll(train)

    sentences = df.groupby("session").agg(F.collect_list("aid").alias("aid_ls"))

    if sample:
        return (
            sentences.select("aid_ls").limit(sample).rdd.map(lambda x: x[0]).collect()
        )
    else:
        return sentences.select("aid_ls").rdd.map(lambda x: x[0]).collect()


def fit_w2v_model(config: Config, df: List[List[int]]):
    """
    This function takes in a list of lists of integers, which represent the items viewed in each session, and a Config object containing the hyperparameters for the Word2Vec model.
    It fits a Word2Vec model to the input data and returns the trained model.

    Parameters:
        config (Config): A Config object containing the hyperparameters for the Word2Vec model.
        df (List[List[int]]): A list of lists of integers, representing the items viewed in each session.

    Returns:
        Word2Vec: The trained Word2Vec model.
    """
    cores = multiprocessing.cpu_count()

    w2v = Word2Vec(
        sentences=df,
        vector_size=config.w2v_vector_size,
        window=config.w2v_window,
        min_count=config.w2v_min_count,
        negative=config.w2v_negative,
        workers=cores,
    )
    return w2v


def save_word_embeddings(config: Config, w2v: Word2Vec):
    """
    This function saves the embeddings of the words in a trained Word2Vec model to a parquet file.

    Parameters:
        config (Config): A Config object containing the file path to save the embeddings to.
        w2v (Word2Vec): A trained Word2Vec model.
    """
    word_embeds = []
    for idx, word in enumerate(w2v.wv.index_to_key):
        embeddings = w2v.wv[idx]
        embeddings = embeddings.tolist()
        word_embeds.append([word, embeddings])

    # Save to spark dataframe
    spark = SparkSession.builder.appName("w2v_model").getOrCreate()
    w2v_embeds_df = spark.sparkContext.parallelize(word_embeds).toDF(
        ["aid", "embeddings"]
    )
    w2v_embeds_df.write.parquet(config.w2v_item_embed_fp, mode="overwrite")


def save_word_neighbors(config: Config, w2v: Word2Vec, n_neighbors: int = 20):
    """
    This function saves the word neighbors of each word in the w2v model, by creating an Annoy index and saving the
    neighbors to a spark dataframe.

    Parameters:
        config (Config) : A Config object containing the file path to save the word neighbors dataframe.
        w2v (Word2Vec) : A trained gensim word2vec model
    """
    # Create an Annoy index on the driver node
    w2v_index = AnnoyIndex(32, "angular")

    # Create index
    for idx, word in enumerate(w2v.wv.index_to_key):
        embeddings = w2v.wv[idx]
        w2v_index.add_item(word, embeddings)

    # Build index
    w2v_index.build(10)

    # Save neighbors
    word_neighbors = []
    for idx, word in enumerate(w2v.wv.index_to_key):
        r = w2v_index.get_nns_by_item(word, n_neighbors)
        word_neighbors.append([word, r])

    # Save to spark dataframe
    spark = SparkSession.builder.appName("w2v_model").getOrCreate()
    neighbors = spark.sparkContext.parallelize(word_neighbors).toDF(
        ["aid", "neighbors"]
    )
    neighbors.write.parquet(config.w2v_neighbors_fp, mode="overwrite")
