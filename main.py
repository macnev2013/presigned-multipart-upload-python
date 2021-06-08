import io
import math
import boto3
import requests
import logging

from pathlib import Path
from botocore.config import Config

logger = logging.getLogger(__name__)

endpoint_url = 'http://localhost:4566'
ACCESS_KEY = 'test'
SECRET_KEY = 'test'

bucket = 'bucket'
key = 'key'

UPLOAD_CHUNK_SIZE = 5 * 1024 * 1024  # 1MB

client = boto3.client(
    's3',
    config=Config(signature_version='s3v4'),
    endpoint_url=endpoint_url,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

response = client.create_bucket(Bucket=bucket)
print('Bucket created: ', bucket)

# initialize multipart upload
upload_id = client.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
print('Logging id generated: ', bucket)

path = Path('./upload.txt')
file_size = path.stat().st_size
num_parts = math.ceil(file_size / UPLOAD_CHUNK_SIZE)
print('File size is', file_size, ' with', num_parts, ' parts.')

urls = []
for part_number in range(num_parts):
    part_number = part_number + 1
    url = client.generate_presigned_url(
        ClientMethod='upload_part',
        Params={
            'Bucket': bucket,
            'UploadId': upload_id,
            'Key': key,
            'PartNumber': part_number,
        },
        ExpiresIn=8600,
    )
    urls.append(url)
    print('Part: ', part_number, ' | URL: ', url)

parts = []
with path.open('rb') as file_:
    for part_idx in range(num_parts):
        chunk = file_.read(UPLOAD_CHUNK_SIZE)
        resp = requests.put(
            urls[part_idx],
            io.BytesIO(chunk),
        )
        parts.append(
            {
                'etag': resp.headers['ETag'],
                'part_number': part_idx + 1,
            }
        )
        resp.raise_for_status()
        print('Part: ', part_idx + 1, ' | Etag: ', resp.headers['ETag'])

client.complete_multipart_upload(
    Bucket=bucket,
    Key=key,
    MultipartUpload={
        'Parts': [
            {'ETag': p['etag'], 'PartNumber': p['part_number']}
            for p in parts
        ]
    },
    UploadId=upload_id,
)
