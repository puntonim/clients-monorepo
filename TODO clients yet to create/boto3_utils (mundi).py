from functools import lru_cache

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_dynamodb import DynamoDBServiceResource
from mypy_boto3_dynamodb.service_resource import Table
from mypy_boto3_s3 import S3Client, S3ServiceResource
from mypy_boto3_sqs import SQSServiceResource
from mypy_boto3_sqs.service_resource import Queue

from ..conf import settings

# Boto3 resources as singleton global vars so they are reused in subsequent function invocations.
# But never import these directly, use the getter functions instead. Otherwise unit tests
#  fail because moto does not mock properly. See conftest.py
# Also we use lazy init to allow for instance something like `os.environ["AWS_PROFILE"] = "289"`.
_dynamodb_resource = None
_table_resource = None
_s3_resource = None
_s3_client = None
_sqs_resource = None
_sqs_queue_resource = None


def get_dynamodb_resource() -> DynamoDBServiceResource:
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb", region_name="us-east-1")
    return _dynamodb_resource


def get_table_resource() -> Table:
    global _table_resource
    # _table_resource is built at module init time. This does not play well with
    #  @override_settings, to solve this we redefine the global var:
    if _table_resource and _table_resource.name != settings.DYNAMODB_TABLE_NAME:
        _table_resource = get_dynamodb_resource().Table(settings.DYNAMODB_TABLE_NAME)
    if _table_resource is None:
        _table_resource = get_dynamodb_resource().Table(settings.DYNAMODB_TABLE_NAME)
    return _table_resource


def get_s3_resource() -> S3ServiceResource:
    global _s3_resource
    if _s3_resource is None:
        _s3_resource = boto3.resource("s3")
    return _s3_resource


def get_s3_client() -> S3Client:
    global _s3_client
    if _s3_client is None:
        _s3_client = get_s3_resource().meta.client
    return _s3_client


def get_sqs_resource() -> SQSServiceResource:
    global _sqs_resource
    if _sqs_resource is None:
        _sqs_resource = boto3.resource("sqs", region_name="us-east-1")
    return _sqs_resource


def get_sqs_queue_resource() -> Queue:
    global _sqs_queue_resource
    # Lazy init _sqs_queue_resource to allow tests to create the queue before
    #  retrieving it by name. If not it will result in QueueDoesNotExist error.
    if _sqs_queue_resource is None:
        _sqs_queue_resource = get_sqs_resource().get_queue_by_name(
            QueueName=settings.S3_BUCKET_SCANNER_SQS_QUEUE_NAME
        )
    return _sqs_queue_resource


@lru_cache
def has_s3_object(s3_bucket_name: str, s3_key: str) -> bool:
    # TODO BAS-2510 use s3_client lib extracted from map-cli instead.
    try:
        # Use Client.head_object directly instead of Object.load. They essentially do the same thing
        #  except that Object.load has some additional `lazy-loading` involved.
        #  Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object
        # When testing both methods across hundreds of concurrent Lambda invocations, Client.head_object
        #  didn't raise any generic error responses while Object.load did.
        get_s3_client().head_object(Bucket=s3_bucket_name, Key=s3_key)
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise
    return True
