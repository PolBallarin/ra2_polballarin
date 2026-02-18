"""
Subir carpeta raw/ (Delta Lake) a S3
=====================================
pip install boto3
"""

import boto3
import os

AWS_ACCESS_KEY = "AWS_ACCESS_KEY"
AWS_SECRET_KEY = "AWS_SECRET_KEY"
BUCKET = "lasalle-bigdata-2025-2026"
S3_PREFIX = "pol_ballarin/raw"
LOCAL_DIR = "raw"

s3 = boto3.client('s3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='eu-west-3'  # Cambiar si es otra región
)

count = 0
for root, dirs, files in os.walk(LOCAL_DIR):
    for file in files:
        local_path = os.path.join(root, file)
        # raw/tags/part-0.parquet → pol_ballarin/raw/tags/part-0.parquet
        s3_key = os.path.join(S3_PREFIX, os.path.relpath(local_path, LOCAL_DIR)).replace("\\", "/")
        
        print(f"  Subiendo {local_path} → s3://{BUCKET}/{s3_key}")
        s3.upload_file(local_path, BUCKET, s3_key)
        count += 1

print(f"\nSubidos {count} archivos a s3://{BUCKET}/{S3_PREFIX}/")