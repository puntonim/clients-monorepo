"""
** DYNAMODB CLIENT TABLE **
===========================

Note: there is also a function create_table() in dynamodb_client_base.py, particularly
 useful in tests. See top docstring in that file.

```py
import aws_dynamodb_client

item = {
    "PK": "BOTTE_MESSAGE",
    "SK": "2XxEn9LlUFuTyn0tOCySn11smMS",  # ksuid.KsuidMs().
    "TaskId": "BOTTE_MESSAGE",
    "SenderService": "DYNAMODB_CLIENT_PYTEST",
    "ExpirationTs": 1699565417,
    "Payload": {
        "text": "Hello world from (clients-monorepo) DynamoDB Client Pytest"
    },
}
table = aws_dynamodb_client.DynamodbTable("botte-be-task-prod")
try:
    response = table.write(item)
except aws_dynamodb_client.BotoAuthError:
    raise
except aws_dynamodb_client.TableDoesNotExist:
    raise
except aws_dynamodb_client.InvalidPutItemMethodParameter:
    raise
except aws_dynamodb_client.PrimaryKeyConstraintError:
    raise
except aws_dynamodb_client.EndpointConnectionError:
    raise
assert response == {
    "ResponseMetadata": {
        "RequestId": "SSAOU4PD9U9NKQ7PVEDBKCJ7KVVV4KQNSO5AEMVJF66Q9ASUAAJG",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "connection": "keep-alive",
            "content-length": "2",
            "content-type": "application/x-amz-json-1.0",
            "date": "Fri, 31 Oct 2025 18:53:59 GMT",
            "server": "Server",
            "x-amz-crc32": "2745614147",
            "x-amzn-requestid": "SSAOU4PD9U9NKQ7PVEDBKCJ7KVVV4KQNSO5AEMVJF66Q9ASUAAJG",
        },
        "RetryAttempts": 0,
    }
}

response = table.read_by_pk(item["PK"])
assert response == {
    "Items": [item],
    "Count": 1,
    "ScannedCount": 1,
    "ResponseMetadata": {
        "RequestId": "9EAD1I8US34CUHE78ICT820P9BVV4KQNSO5AEMVJF66Q9ASUAAJG",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "connection": "keep-alive",
            "content-length": "312",
            "content-type": "application/x-amz-json-1.0",
            "date": "Fri, 31 Oct 2025 19:15:35 GMT",
            "server": "Server",
            "x-amz-crc32": "2687463111",
            "x-amzn-requestid": "9EAD1I8US34CUHE78ICT820P9BVV4KQNSO5AEMVJF66Q9ASUAAJG",
        },
        "RetryAttempts": 0,
    },
}

response = table.read_all()
assert response == {
    "Items": [item],
    "Count": 1,
    "ScannedCount": 1,
    "ResponseMetadata": {
        "RequestId": "TT315IPDK2G7LM0522RTHR9L7JVV4KQNSO5AEMVJF66Q9ASUAAJG",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "content-type": "application/x-amz-json-1.0",
            "x-amzn-requestid": "TT315IPDK2G7LM0522RTHR9L7JVV4KQNSO5AEMVJF66Q9ASUAAJG",
            "content-length": "312",
            "date": "Fri, 31 Oct 2025 20:04:48 GMT",
            "server": "Server",
            "connection": "keep-alive",
            "x-amz-crc32": "2687463111",
        },
        "RetryAttempts": 0,
    },
}
```
"""

import functools

import log_utils as logger
from boto3.dynamodb.conditions import Key
from botocore import exceptions as botocore_exceptions
from mypy_boto3_dynamodb.service_resource import Table
from mypy_boto3_dynamodb.type_defs import (
    PutItemOutputTableTypeDef,
    QueryOutputTableTypeDef,
    ScanOutputTableTypeDef,
)

from . import dynamodb_client_base_exceptions
from . import dynamodb_client_table_exceptions as exceptions
from .dynamodb_client_base import DynamodbClientBase

__all__ = ["DynamodbTable"]


def handle_common_exceptions(fn):
    @functools.wraps(fn)
    def closure(*fn_args, **fn_kwargs):
        # `self` in the original method, if the decorated fn is a method.
        # zelf = fn_args[0]
        try:
            return fn(*fn_args, **fn_kwargs)
        except botocore_exceptions.NoCredentialsError as exc:
            raise dynamodb_client_base_exceptions.BotoAuthError from exc
        except botocore_exceptions.ClientError as exc:
            if (
                "ExpiredTokenException" in exc.response.get("Error", {}).get("Code", "")
                or "ExpiredTokenException" in str(exc)
                or "UnrecognizedClientException"
                in exc.response.get("Error", {}).get("Code", "")
                or "UnrecognizedClientException" in str(exc)
            ):
                raise dynamodb_client_base_exceptions.BotoAuthError from exc
            elif "ResourceNotFoundException" in exc.response.get("Error", {}).get(
                "Code", ""
            ) or "ResourceNotFoundException" in str(exc):
                raise exceptions.TableDoesNotExist from exc
            raise

    return closure


class DynamodbTable(DynamodbClientBase):
    def __init__(self, name: str, *args, **kwargs) -> None:
        """
        Client for AWS DynamoDB Table API.

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
        Boto3 DynamoDB Table docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html

        Args:
            name: eg. "mytable".
        """
        super().__init__(*args, **kwargs)
        self.name: str = name
        self.table: Table = self.dynamodb_resource.Table(name)

    @functools.cached_property
    @handle_common_exceptions
    def key_attr_names(self):
        key_attr_names = dict(hash=None, range=None)
        for x in self.table.key_schema:
            # self.table.key_schema is a list like: [{'AttributeName': 'TaskId', 'KeyType': 'HASH'}, {'AttributeName': 'Text', 'KeyType': 'RANGE'}]
            key_attr_names[x["KeyType"].lower()] = x["AttributeName"]

        return key_attr_names

    @handle_common_exceptions
    def write(
        self, item: dict, do_overwrite_existing: bool = False
    ) -> PutItemOutputTableTypeDef:
        """
        Write (put) an item to the table.

        Args:
            item (dict): the actual item.
            do_overwrite_existing: True to raise PrimaryKeyConstraintError if an item
             already exists with the same primary and sort key.

        Returns: PutItemOutputTableTypeDef object.
        """
        kwargs = {"Item": item}
        if not do_overwrite_existing:
            pk_attr_name = self.key_attr_names["hash"]
            # Do not overwrite existing records.
            # Do not use `Attr("PK").not_exists()` as it does not work with
            #  transact_write_items().
            # Note that the primary key is enough for this check (no need to add also
            #  the sort key).
            kwargs["ConditionExpression"] = f"attribute_not_exists({pk_attr_name})"

        try:
            # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item
            response: PutItemOutputTableTypeDef = self.table.put_item(**kwargs)
        except (
            self.dynamodb_client.exceptions.TableNotFoundException,
            self.dynamodb_client.exceptions.ResourceNotFoundException,
        ) as exc:
            raise exceptions.TableDoesNotExist from exc
        except botocore_exceptions.ClientError as exc:
            if "ValidationException" in exc.response.get("Error", {}).get(
                "Code", ""
            ) or "ValidationException" in str(exc):
                raise exceptions.InvalidPutItemMethodParameter from exc
            elif "ConditionalCheckFailedException" in exc.response.get("Error", {}).get(
                "Code", ""
            ) or "ConditionalCheckFailedException" in str(exc):
                raise exceptions.PrimaryKeyConstraintError(item) from exc
            raise
        except self.dynamodb_client.exceptions.ConditionalCheckFailedException as exc:
            raise exceptions.PrimaryKeyConstraintError(item) from exc
        except botocore_exceptions.EndpointConnectionError as exc:
            raise dynamodb_client_base_exceptions.EndpointConnectionError from exc
        except (botocore_exceptions.ParamValidationError, TypeError) as exc:
            logger.error(
                "Invalid parameter to the API put_item method, eg. Item is not a dict",
            )
            raise exceptions.InvalidPutItemMethodParameter from exc
        # except (
        #     botocore_exceptions.BotoCoreError,
        #     botocore_exceptions.ClientError,
        # ) as exc:
        #     raise exceptions.GenericBotoError from exc

        return response

    @handle_common_exceptions
    def read_by_pk(
        self, pk_value: str, pk_attr_name: str = "PK"
    ) -> QueryOutputTableTypeDef:
        """
        Read items by PK. It's a cheap query operation.
        """
        # TODO implement pagination.
        try:
            items = self.table.query(
                KeyConditionExpression=Key(pk_attr_name).eq(pk_value),
            )
        except botocore_exceptions.ClientError as exc:
            if "Query condition missed key schema element" in exc.response.get(
                "Error", {}
            ).get("Message", ""):
                raise exceptions.PkAttrNameInvalid(pk_attr_name) from exc
            elif (
                "Condition parameter type does not match schema type"
                in exc.response.get("Error", {}).get("Message", "")
            ):
                raise exceptions.PkAttrValueNonString(pk_value) from exc
            raise
        return items

    @handle_common_exceptions
    def read_all(self) -> ScanOutputTableTypeDef:
        """
        Read all items. It is an expensive scan operation.
        """
        # TODO implement pagination.
        items = self.table.scan()
        return items
