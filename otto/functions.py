from pyspark.sql import functions as F


def cosine_similarity(df, col1, col2):
    """
    This function calculates the cosine similarity between two columns of a DataFrame.
    The cosine similarity is a measure of similarity between two non-zero vectors of an inner product space that measures the cosine of the angle between them.

    Steps:
    - The function first calculates the dot product between the two columns, which is a measure of how similar the two columns are.
    - Then it calculates the norm of each column, which is the length of the column.
    - Finally, it calculates the cosine similarity by dividing the dot product by the product of the norms.

    Note:
        - col1 and col2 must be arrays. If they are in vector format, use the `vector_to_array` function
        to convert them to arrays.
        e.g.
        ```
        from pyspark.ml.functions import vector_to_array
        df.withColumn(col1, vector_to_array(col1)
        ```
    """
    # multiplying each position in the two embedding arrays and accumulating the summed value
    dot = f"aggregate(arrays_zip({col1}, {col2}), 0D, (acc, x) -> acc + (x.{col1} * x.{col2}))"

    # squaring each position in the embedding and accumulating the value
    norm1 = f"sqrt(aggregate({col1}, 0D, (acc, x) -> acc + (x * x)))"

    # squaring each position in the embedding being compared and accumulating the value
    norm2 = f"sqrt(aggregate({col2}, 0D, (acc, x) -> acc + (x * x)))"

    # cosine formula: dot product divided by product of each vector's magnitude
    cosine = f"{dot} / ({norm1} * {norm2})"

    return df.withColumn("cosine", F.expr(cosine))
