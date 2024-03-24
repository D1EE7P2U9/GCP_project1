from google.cloud import storage
from faker import Faker
import os
from dotenv import load_dotenv
import pandas as pd
import io
from datetime import datetime
from google.oauth2 import service_account

# Load environment variables from .env file
load_dotenv()

PROJECT_ID = os.getenv('PROJECT_ID')
SERVICE_ACCOUNT_KEY_PATH = os.getenv('SERVICE_ACCOUNT_KEY_PATH')
bucket_name = os.getenv('GCS_BUCKET_NAME')

credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_KEY_PATH,
    scopes=['https://www.googleapis.com/auth/cloud-platform']
)

# Initialize GCS client with explicit credentials
client = storage.Client(project=PROJECT_ID,credentials=credentials)

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=SERVICE_ACCOUNT_KEY_PATH

# Initialize the GCS client
client = storage.Client(project=PROJECT_ID, credentials=credentials)


def generate_fake_data(num_rows):
    """Generate fake data using Faker library."""
    fake = Faker()
    data = {
        "id": [fake.random_int(min=1, max=1000) for _ in range(num_rows)],
        "name": [fake.name() for _ in range(num_rows)],
        "item": [fake.word() for _ in range(num_rows)]
    }
    return pd.DataFrame(data)

def upload_dataframe_to_gcs(bucket_name, dataframe, destination_blob_name):
    """Uploads a Pandas DataFrame to a GCS bucket as a CSV file."""
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        csv_data = io.StringIO()
        dataframe.to_csv(csv_data, index=False)
        blob.upload_from_string(csv_data.getvalue(), content_type="csv")
        print(f"DataFrame uploaded to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"Error uploading DataFrame: {e}")

# Example usage
if __name__ == "__main__":
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    num_rows = 100
    current_date = datetime.now().strftime("%d-%m-%Y")
    
    destination_blob_name = f"consumer/consumer_data_{current_date}.csv"

    # Generate fake data
    fake_data = generate_fake_data(num_rows)

    # Upload fake data to GCS
    upload_dataframe_to_gcs(bucket_name, fake_data, destination_blob_name)
