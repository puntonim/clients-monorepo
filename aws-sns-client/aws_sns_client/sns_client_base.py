from abc import ABC

import boto3
from botocore.exceptions import PartialCredentialsError, ProfileNotFound
from mypy_boto3_sns.client import SNSClient
from mypy_boto3_sns.service_resource import SNSServiceResource

from . import sns_client_base_exceptions as exceptions

__all__ = [
    "create_topic",
    "REGION_NAME_N_VIRGINIA",
    "REGION_NAME_SINGAPORE",
    "REGION_NAME_MILAN",
    "REGION_NAME_DEFAULT",
]

REGION_NAME_N_VIRGINIA = "us-east-1"
REGION_NAME_SINGAPORE = "ap-southeast-1"
REGION_NAME_MILAN = "eu-south-1"
REGION_NAME_DEFAULT = REGION_NAME_MILAN


class SnsClientBase(ABC):  # noqa: B024
    def __init__(self, aws_region_name: str | None = None) -> None:
        """
        Client for AWS SNS.
        This class is not meant to be used alone, but rather to be used as parent class.

        Concurrency.
        It is meant for interacting with SNS non-concurrently, so do not use the
        same instance in multiple threads concurrently (as resource instances are
        not thread-safe: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-and-multiprocessing).
        Build something similar to what `S3ClientLowLevel` is for S3, for concurrent usage.

        Authentication.
        It requires AWS credentials on the machine (like env vars or ~/.aws/credentials).
        Note: use the env var AWS_PROFILE to use a profile different from default,
         eg. AWS_PROFILE=289 pytest -s tests.

        It's a wrapper around the Boto3 library.
        Boto3 docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html

        Args:
            aws_region_name: AWS region name, eg. "us-east-1".
        """
        if aws_region_name is None:
            aws_region_name = REGION_NAME_DEFAULT

        try:
            self._session = boto3.session.Session(region_name=aws_region_name)
            # Note: when doing multi-threading do NOT share the Boto3 resource, but use
            # and share a low-level client instead, see:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-and-multiprocessing
            self.sns_resource: SNSServiceResource = self._session.resource("sns")
        except (PartialCredentialsError, ProfileNotFound) as exc:
            raise exceptions.BotoAuthError from exc

        # To access the underlying low-level client:
        self.sns_client: SNSClient = self.sns_resource.meta.client


def create_topic(topic_name: str, region_name: str | None = None) -> str:
    if not region_name:
        region_name = REGION_NAME_DEFAULT
    client = boto3.client("sns", region_name=region_name)
    response = client.create_topic(Name=topic_name)
    topic_arn = response["TopicArn"]
    return topic_arn
