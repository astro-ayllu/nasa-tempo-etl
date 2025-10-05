import boto3
import json
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.utils.logger import logger
import src.config as config

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
AWS_REGION = config.AWS_REGION
AWS_BUCKET_NAME= config.AWS_BUCKET_NAME


WORKERS = 64

def _upload_json(data, bucket_name, object_key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    try:
        json_bytes = json.dumps(data).encode('utf-8')
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json_bytes,
            ContentType='application/json',
            ACL='public-read'
        )
        return True
    except ClientError as e:
        logger.error(f"Error subiendo a S3: {e}")
        return False

def save_files(files, bucket_name=AWS_BUCKET_NAME):
    def upload_one(file):
        filename = file['filename']
        content = file['content']
        object_key = filename
        result = _upload_json(content, bucket_name, object_key)
        if result:
            logger.info(f"Archivo {filename} subido exitosamente a {bucket_name}/{object_key}")
            return filename
        else:
            logger.error(f"Error subiendo el archivo {filename} a {bucket_name}/{object_key}")
            raise Exception(f"Error subiendo {filename}")

    with ThreadPoolExecutor(max_workers=min(WORKERS, len(files))) as ex:
        futs = [ex.submit(upload_one, file) for file in files]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception as e:
                logger.error(f"FAIL: {e}")

def save_single_file(key, content, bucket_name=AWS_BUCKET_NAME):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=content,
        ContentType='text/csv',
        ACL='public-read'
    )

    logger.info(f'Archivo guardado en s3://{bucket_name}/{key}')

