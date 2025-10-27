import boto3
from mypy_boto3_dynamodb import DynamoDBServiceResource
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_dynamodb.service_resource import Table

from ..conf import settings

# Boto3 resources as singleton global vars so they are reused in subsequent function invocations.
# But never import these directly, use the getter functions instead. Otherwise unit tests
#  fail because moto does not mock properly. See conftest.py
# Also we use lazy init to allow for instance something like `os.environ["AWS_PROFILE"] = "289"`.
_dynamodb_resource = None
_table_resource = None


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


def get_dynamodb_client() -> DynamoDBClient:
    return get_table_resource().meta.client
