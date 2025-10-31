"""
** DYNAMODB CLIENT BASE **
==========================

See docstring in create_table().
"""

from abc import ABC

import boto3
from botocore.exceptions import PartialCredentialsError, ProfileNotFound
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table

from . import dynamodb_client_base_exceptions as exceptions

__all__ = ["create_table"]


REGION_NAME_N_VIRGINIA = "us-east-1"
REGION_NAME_SINGAPORE = "ap-southeast-1"
REGION_NAME_MILAN = "eu-south-1"
REGION_NAME_DEFAULT = REGION_NAME_MILAN


class DynamodbClientBase(ABC):  # noqa: B024
    def __init__(self, aws_region_name: str | None = None) -> None:
        """
        Client for AWS DynamoDB.
        This class is not meant to be used alone, but rather to be used as parent class.

        Concurrency.
        It is meant for interacting with DynamoDB non-concurrently, so do not use the
        same instance in multiple threads concurrently (as resource instances are
        not thread-safe: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-and-multiprocessing).
        Build something similar to what `S3ClientLowLevel` is for S3, for concurrent usage.

        Authentication.
        It requires AWS credentials on the machine (like env vars or ~/.aws/credentials).
        Note: use the env var AWS_PROFILE to use a profile different from default,
         eg. AWS_PROFILE=289 pytest -s tests.

        It's a wrapper around the Boto3 library.
        Boto3 docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html

        Args:
            aws_region_name: AWS region name, eg. "eu-south-1".
        """
        if aws_region_name is None:
            aws_region_name = REGION_NAME_DEFAULT

        try:
            self._session = boto3.session.Session(region_name=aws_region_name)
            # Note: when doing multi-threading do NOT share the Boto3 resource, but use
            #  and share a low-level client instead, see:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-and-multiprocessing
            self.dynamodb_resource: DynamoDBServiceResource = self._session.resource(
                "dynamodb"
            )
        except (PartialCredentialsError, ProfileNotFound) as exc:
            raise exceptions.BotoAuthError from exc

        # To access the underlying low-level client:
        self.dynamodb_client: DynamoDBClient = self.dynamodb_resource.meta.client


def create_table(
    table_name: str, tags: dict[str, str] | None = None, region_name: str | None = None
) -> Table:
    """
    Create a DynamoDB table.
    Typically used in conftest.py with moto to mock DynamoDB; while the real prod table
     is typically created with Serverless or Terraform or similar..

    Example of usage in conftest.py
        ```py
        @pytest.fixture(autouse=True, scope="function")
        def dynamodb_mock(request):
            \"""
            Use moto as a DynamoDB mock in all tests.
            Unless the test function/class/module is marked with:
                `@pytest.mark.nomoto` for functions and classes
                `pytestmark = pytest.mark.nomoto` for modules.

            Write tests reading and writing to DynamoDB with your regular prod code that
             now will write to the mock instead.
            \"""
            if "nomoto" in request.keywords:
                yield
                return
            with mock_dynamodb():
                table_resource = dynamodb_client_base.create_table(
                    "alarm-be-pytest-mock-dynamodb-table"
                )
                with override_settings(
                    settings,
                    BOTTE_MESSAGE_DYNAMODB_TASK_TABLE_NAME=table_resource.table_name,
                ):
                    yield
        ```
    """
    region_name = region_name or REGION_NAME_DEFAULT

    attribute_definitions = [
        {"AttributeName": "PK", "AttributeType": "S"},
        {"AttributeName": "SK", "AttributeType": "S"},
    ]

    tags = tags or {}
    tag_list = []
    for tag_name, tag_value in tags.items():
        tag_list.append(
            {"Key": tag_name, "Value": tag_value},
        )

    # Load dynamodb resource from boto3 directly (instead of using the global var
    #  singleton in utils.boto3_utils) so this code can be used in conftest.py
    #  with moto mocking DynamoDB.
    dynamodb_resource = boto3.resource("dynamodb", region_name=region_name)
    # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table.
    table_resource = dynamodb_resource.create_table(
        TableName=table_name,
        AttributeDefinitions=attribute_definitions,
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        # GlobalSecondaryIndexes=global_secondary_indexes,
        BillingMode="PAY_PER_REQUEST",
        Tags=tag_list,
        # Tags=[
        #     {"Key": "product", "Value": tag_product},
        #     {"Key": "stage", "Value": stage},
        # ],
    )
    table_resource.wait_until_exists()

    return table_resource
