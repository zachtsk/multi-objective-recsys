# Spark 3.3 & Python 3.8+
FROM jupyter/pyspark-notebook:python-3.10

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Dependencies
RUN pip install pyspark

WORKDIR /app

# Move into app directory
RUN mkdir -p otto
COPY otto /app/otto

# Install python package as editable
COPY setup.* .
RUN pip install -e .
