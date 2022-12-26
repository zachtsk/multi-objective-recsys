######################
# Data Engineering
######################
### Normalize dataset
# Example usage to normalize the test data (for subset of records):
# ./brain/scratch.py normalize -t 'test' -s 'sm'
normalize_test:
	docker-compose run --rm pyspark spark-submit ./brain/scratch.py normalize -t 'test'
normalize_train:
	docker-compose run --rm pyspark spark-submit ./brain/scratch.py normalize -t 'train'

#CLUSTER_NAME = kaggle-spark
#IMAGE_NAME = gcr.io/txt-save-dev/multi-obj-spark:v1.0
#REGION = us-central1
#
## https://cloud.google.com/dataproc/docs/guides/dpgke/dataproc-gke-custom-images
#cluster:
#	gcloud dataproc clusters gke create $(CLUSTER_NAME)\
#		--gke-cluster-location=$(REGION)\
#		--spark-engine-version=2.0\
#		--properties=spark:spark.kubernetes.container.image=$(IMAGE_NAME)
#
#job:
#	gcloud dataproc jobs submit pyspark /app/brain/scratch.py --cluster=$(CLUSTER_NAME) --region=$(REGION)
#
#down:
#	gcloud dataproc clusters delete $(CLUSTER_NAME) --region=$(REGION)
