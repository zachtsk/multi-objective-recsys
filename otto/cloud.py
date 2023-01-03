import argparse
from pathlib import Path

from google.cloud import storage
from google.cloud.storage import Bucket


def create_gcs_bucket(bucket_id: str) -> None:
    """Create a bucket for storing files"""

    # Initialize client
    client = storage.Client()

    # Try to create bucket
    try:
        bucket = client.create_bucket(bucket_id)
        print(f"\nCreated bucket `{bucket.id}`")

    # Bucket already exists
    except Exception as inst:
        print(f"\nBucket `{bucket_id}` already exists.\nDetails:")
        print(f"\t{inst}")


def upload_data_to_gcs(bucket_id: str, folder: str = None) -> None:
    """Upload files in data directory to gcs"""

    # Initialize client
    client = storage.Client()
    bucket = client.bucket(bucket_id)

    # Navigate to data folder
    data_folder = Path(__file__).parent.parent/'data'
    competition = 'multi-obj-recsys'

    # Upload each file
    for i,file in enumerate(data_folder.glob("*")):
        src_filepath = file.absolute()
        bucket_filepath = f"{competition}/{file.name}"

        # Check if blob already exists, skip if it does
        blob = bucket.blob(bucket_filepath)
        if blob.exists():
            print(f"Blob `gs://{bucket_id}/{bucket_filepath}` already exists")
            continue

        # Upload file to bucket
        print(f"Begin uploading `gs://{bucket_id}/{bucket_filepath}`")
        blob.upload_from_filename(src_filepath)
        print(f"Completed uploading `gs://{bucket_id}/{bucket_filepath}`")


def main():
    """
    Command line tool for setting up Google Cloud Services

    Examples:
        # Create GCS Bucket
        > python -m cloud create -b {bucket_name}

        # Upload data files to GCS Bucket
        > python -m cloud upload -b {bucket_name}
    """

    # Init parser
    parser = argparse.ArgumentParser()

    # Ordinal argument for the function to select
    # OPTIONS: ['create', 'upload']
    parser.add_argument("function")

    # Parameters
    parser.add_argument("-b", "--bucket_id")

    # Parse arguments
    args = parser.parse_args()

    # Dispatch functions
    if args.function == "create":
        create_gcs_bucket(args.bucket_id)
    elif args.function == "upload":
        upload_data_to_gcs(args.bucket_id, args.path)


if __name__ == "__main__":
    main()
