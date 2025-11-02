"""
** SNS CLIENT TOPIC **
======================

```py
import aws_sns_client

client = aws_sns_client.SnsTopic("arn:aws:sns:eu-south-1:477353422995:aws-watchdog-errors-prod")
client.publish(self.body)
```
"""

import json

import json_utils
import log_utils as logger
from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError
from mypy_boto3_sns.service_resource import Topic

from . import sns_client_base_exceptions
from . import sns_client_topic_exceptions as exceptions
from .sns_client_base import SnsClientBase

__all__ = ["SnsTopic"]


class SnsTopic(SnsClientBase):
    def __init__(self, arn: str, *args, **kwargs) -> None:
        """
        Client for AWS SNS Topic API.

            Concurrency.
            It is meant for interacting with SNS non-concurrently, so do not use the
            same instance in multiple threads concurrently (as resource instances are
            not thread-safe: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-and-multiprocessing).
            Build something similar to what `S3ClientLowLevel` is for S3, for concurrent usage.

            Authentication.
            It requires AWS credentials on the machine (like env vars or ~/.aws/credentials).
            Note: use the env var AWS_PROFILE to use a profile different from default,
             eg. AWS_PROFILE=289 pytest -s tests.

        Boto3 docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#topic

        Args:
            arn: eg. "arn:aws:sns:ap-southeast-1:289485838881:hdmap-services-events".
        """
        super().__init__(*args, **kwargs)
        self.arn: str = arn
        self.topic: Topic = self.sns_resource.Topic(arn)

    def publish(self, body: str | dict, do_set_json_content_type=True) -> dict:
        """
        Publish a message to the SNS topic.

        Args:
            body: string or dict.
            do_set_json_content_type: if true then body is converted to json and
             a proper attribute is added.

        Returns: a dict like:
            {
                "MessageId": "1d336a2c-8e7e-5a71-9b9c-30b7444187a2",
                "ResponseMetadata": {
                    "RequestId": "6f4ff116-9502-5ad2-b8bf-ab8d5f3da701",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "x-amzn-requestid": "6f4ff116-9502-5ad2-b8bf-ab8d5f3da701",
                        "content-type": "text/xml",
                        "content-length": "294",
                        "date": "Fri, 04 Mar 2022 08:04:14 GMT"
                    },
                    "RetryAttempts": 0
                }
            }
        """
        attributes = None
        if do_set_json_content_type:
            try:
                body = (
                    json.dumps(body, cls=json_utils.CustomJsonEncoder)
                    if (isinstance(body, (dict, list)))
                    else body
                )
            except TypeError as exc:
                logger.error(f"Body non JSON serializable: {body}")
                raise exceptions.NotJsonSerializable from exc
            attributes = {
                "content_type": {
                    "DataType": "String",
                    "StringValue": "application/json",
                }
            }

        logger.debug(f"Publishing message to {self.arn}", extra=dict(body=body))

        kwargs = dict(
            # TargetArn: str,
            # PhoneNumber: str,
            Message=body,
            # Subject: str,
            # MessageStructure: str,
            # MessageAttributes: dict,
            # MessageDeduplicationId: str,
            # MessageGroupId: str,
        )
        if attributes:
            kwargs["MessageAttributes"] = attributes

        try:
            response: dict = self.topic.publish(**kwargs)
        except NoCredentialsError as exc:
            logger.error("No AWS credentials found")
            raise sns_client_base_exceptions.BotoAuthError(
                "NoCredentialsError"
            ) from exc
        except (
            self.sns_client.exceptions.InvalidParameterException,
            self.sns_client.exceptions.NotFoundException,
        ) as exc:
            logger.error("Topic not found", extra=dict(arn=self.arn))
            raise exceptions.TopicNotFound(topic_arn=self.arn) from exc
        except ParamValidationError as exc:
            logger.error(
                "Invalid parameter to the publish method, eg. Message is not a string"
            )
            raise exceptions.InvalidPublishMethodParameter from exc
        except ClientError as exc:
            logger.error("AWS token expired")
            if exc.response["Error"]["Code"] == "ExpiredToken":
                raise sns_client_base_exceptions.BotoAuthErrorTokenExpired from exc
            elif exc.response["Error"]["Code"] == "InvalidClientTokenId":
                logger.error("AWS authentication error: InvalidClientTokenId")
                raise sns_client_base_exceptions.BotoAuthError(
                    "InvalidClientTokenId"
                ) from exc
            logger.error("Generic error response")
            raise sns_client_base_exceptions.BaseErrorResponse from exc

        logger.debug("Successful response", extra=dict(response=response))

        # response like:
        # {'MessageId': '8e7fc102-37c5-555a-aba0-a08351f117e8',
        #  'ResponseMetadata': {'RequestId': 'ba14fb35-2ab2-56a0-ba6f-9ab44c862412',
        #                       'HTTPStatusCode': 200,
        #                       'HTTPHeaders': {'content-length': '294',
        #                                       'content-type': 'text/xml',
        #                                       'date': 'Sun, 02 Nov 2025 14:00:20 GMT',
        #                                       'connection': 'keep-alive',
        #                                       'x-amzn-requestid': 'ba14fb35-2ab2-56a0-ba6f-9ab44c862412'},
        #                       'RetryAttempts': 0}}
        return response
