import boto3
import json
from botocore.exceptions import ClientError
from src.utils.logger import logger
import src.config as config

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
AWS_REGION = config.AWS_REGION
AWS_BUCKET_NAME= config.AWS_BUCKET_NAME


def _upload_json(data, bucket_name, object_key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
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
    for file in files:
        filename = file['filename']
        content = file['content']
        object_key = filename
        success = _upload_json(content, bucket_name, object_key)
        if success:
            logger.info(f"Archivo {filename} subido exitosamente a {bucket_name}/{object_key}")
        else:
            logger.error(f"Error subiendo el archivo {filename} a {bucket_name}/{object_key}")
