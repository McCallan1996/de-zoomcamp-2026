# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "google-cloud-storage>=3.9.0",
# ]
# ///
import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden #google-cloud-storage
import time

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
# TODO: เปลี่ยนชื่อ Bucket เป็นชื่อที่คุณต้องการ (ต้องไม่ซ้ำใครทั่วโลก)
BUCKET_NAME = "x" 

# URL สำหรับโหลดข้อมูล Yellow Taxi ปี 2024 เดือน 1-6
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)] # สร้าง list ["01", "02", ..., "06"]
DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024 # 8 MB

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ---------------------------------------------------------
# AUTHENTICATION (GOOGLE SDK)
# ---------------------------------------------------------
# เราใช้ SDK Auth ดังนั้นไม่ต้องระบุ json file
# Code จะดึง Credential จาก 'gcloud auth application-default login' อัตโนมัติ
print("Authenticating using Google SDK (Application Default Credentials)...")
client = storage.Client() 

# ---------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------

def create_bucket_if_not_exists(bucket_name):
    """สร้าง Bucket หากยังไม่มี"""
    try:
        bucket = client.bucket(bucket_name)
        bucket.reload()
        print(f"Bucket '{bucket_name}' exists using it...")
        return bucket
    except NotFound:
        print(f"Bucket '{bucket_name}' not found. Creating...")
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
        return bucket
    except Forbidden:
        print(f"Error: Bucket name '{bucket_name}' is already taken by someone else or restricted.")
        sys.exit(1)

def download_file(month):
    """ดาวน์โหลดไฟล์ Parquet จาก URL ลงเครื่อง"""
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def verify_gcs_upload(bucket, blob_name):
    """ตรวจสอบว่าไฟล์ขึ้นไปอยู่บน GCS จริงไหม"""
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(bucket, file_path, max_retries=3):
    """อัปโหลดไฟล์ขึ้น GCS พร้อมระบบ Retry"""
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {bucket.name} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{bucket.name}/{blob_name}")

            if verify_gcs_upload(bucket, blob_name):
                print(f"Verification successful for {blob_name}")
                # ลบไฟล์ในเครื่องทิ้งเพื่อประหยัดพื้นที่ (Optional)
                os.remove(file_path) 
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    # 1. เตรียม Bucket
    bucket = create_bucket_if_not_exists(BUCKET_NAME)

    # 2. ดาวน์โหลดไฟล์ (Parallel Download)
    print(f"Starting download for months: {MONTHS}")
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    # 3. อัปโหลดไฟล์ (Parallel Upload)
    print("Starting upload to GCS...")
    valid_files = list(filter(None, file_paths))
    
    # ใช้ lambda เพื่อส่ง bucket object เข้าไปในฟังก์ชัน
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(lambda f: upload_to_gcs(bucket, f), valid_files)

    print("\nAll operations completed. Please check your GCS bucket.")