# OPEN CORE - ADD
import boto3
from shared.settings import settings

from .data_tools_core_s3 import Config
from .data_tools_core_s3 import DataToolsS3


class DataToolsMinio(DataToolsS3):
    """
        Data tools Implementation for Minio S3. Handles Upload and download
        of blobs from S3 Buckets.

        Requires setting the following settings
        - DIFFGRAM_MINIO_ENDPOINT_URL
        - DIFFGRAM_MINIO_ACCESS_KEY_ID
        - DIFFGRAM_MINIO_ACCESS_KEY_SECRET
        - DIFFGRAM_MINIO_DISABLED_SSL_VERIFY
        - DIFFGRAM_S3_BUCKET_REGION
        - DIFFGRAM_S3_BUCKET_NAME

    """

    def __init__(self):
        if settings.IS_DIFFGRAM_S3_V4_SIGNATURE:
            self.s3_client = boto3.client('s3', config=Config(signature_version='s3v4'),
                                        endpoint_url=settings.DIFFGRAM_MINIO_ENDPOINT_URL,
                                        aws_access_key_id=settings.DIFFGRAM_MINIO_ACCESS_KEY_ID,
                                        aws_secret_access_key=settings.DIFFGRAM_MINIO_ACCESS_KEY_SECRET,
                                        region_name=settings.DIFFGRAM_S3_BUCKET_REGION,
                                        verify=settings.DIFFGRAM_MINIO_DISABLED_SSL_VERIFY)
        else: 
            self.s3_client = boto3.client('s3',
                                        endpoint_url=settings.DIFFGRAM_MINIO_ENDPOINT_URL,
                                        aws_access_key_id=settings.DIFFGRAM_MINIO_ACCESS_KEY_ID,
                                        aws_secret_access_key=settings.DIFFGRAM_MINIO_ACCESS_KEY_SECRET,
                                        region_name=settings.DIFFGRAM_S3_BUCKET_REGION,
                                        verify=settings.DIFFGRAM_MINIO_DISABLED_SSL_VERIFY)
        self.s3_bucket_name = settings.DIFFGRAM_S3_BUCKET_NAME
        self.s3_bucket_name_ml = settings.ML__DIFFGRAM_S3_BUCKET_NAME
        self.s3_expiration_offset = settings.DIFFGRAM_S3_EXPIRATION_OFFSET

    def build_secure_url(self, blob_name: str, expiration_offset: int = None, bucket: str = "web"):
        """
            Builds a presigned URL to access the given blob path.
        :param blob_name: The path to the blob for the presigned URL
        :param expiration_offset: The expiration time for the presigned URL
        :param bucket: string for the bucket type (either 'web' or 'ml') defaults to 'web'
        :return: the string for the presigned url
        """
        # Minio Expiry time is less than 7 days
        if expiration_offset is None:
            expiration_offset = 604800

        if expiration_offset and expiration_offset > 604800:
            raise ValueError('Minio Expiry time is less than 7 days')

        return super().build_secure_url(blob_name, expiration_offset, bucket)
