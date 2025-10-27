import uuid
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Any, Optional

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from ..conf import settings

# TODO consider making a dynamodb_client lib out of this module.


@lru_cache
def dynamize(text: str) -> str:
    """
    Convert a string from Python snake case to DynamoDB format.
    To be used for DynamoDB attribute names.

    Examples:
        gsi_dataset_summary_sk_latest_service_run_at > GSIDatasetSummarySKLatestServiceRunAt
    """
    result = list()
    for tk in text.split("_"):
        if tk.lower() in ("gsi", "pk", "sk"):
            result.append(tk.upper())
        else:
            result.append(tk.title())
    return "".join(result)


@lru_cache
def dedynamize(text: str) -> str:
    """
    Convert a string from DynamoDB attribute name format to Python snake case.
    To be used for DynamoDB attribute names.

    Examples:
        GSIDatasetSummarySKLatestServiceRunAt > gsi_dataset_summary_sk_latest_service_run_at
    """
    text = text.replace("GSI", "_gsi")
    text = text.replace("PK", "_pk")
    text = text.replace("SK", "_sk")

    result = list()
    for c in text:
        if c.isupper():
            result.append("_")
            result.append(c.lower())
            continue
        result.append(c)
    if result[0] == "_":
        result[:] = result[1:]
    return "".join(result)


def serialize_to_dynamodb(obj: Any) -> Any:
    """
    Convert any object to a valid DynamoDB value format.

    Examples
        datetime.datetime(2022, 5, 24, 10, 11, 12, 123456, tzinfo=datetime.timezone.utc) > "2022-05-24T10:11:12.123456+00:00"
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, Enum):
        return obj.value
    elif isinstance(obj, dict):
        # Convert all values from python type to dynamodb type.
        # Example: {'speed_limit_mps': Decimal('20')} -> {'speed_limit_mps': {'N': '20'}}
        dynamodb_dict = dict()
        for key, value in obj.items():
            dynamodb_dict[key] = TypeSerializer().serialize(value)
        return dynamodb_dict
    return obj


def deserialize_from_dynamodb(obj: Any) -> Any:
    """
    Convert any object from a valid DynamoDB value format to Python.

    Examples:
        "2022-05-24T10:11:12.123456+00:00" > datetime.datetime(2022, 5, 24, 10, 11, 12, 123456, tzinfo=datetime.timezone.utc)
    """
    # Not too smart trick to deserialize a string to datetime.
    if isinstance(obj, str) and obj.startswith("20"):
        try:
            return datetime.fromisoformat(obj)
        except ValueError:
            pass
    elif isinstance(obj, dict):
        # Convert all values from dynamodb type to python type.
        # Example: {'speed_limit_mps': {'N': '20'}} -> {'speed_limit_mps': Decimal('20')}
        python_dict = dict()
        for key, value in obj.items():
            python_dict[key] = TypeDeserializer().deserialize(value)
        return python_dict
    return obj


def get_pagination_config(
    do_read_all_items=False, starting_token: Optional[str] = None
) -> dict:
    config = dict()
    limit = settings.DYNAMODB_DEFAULT_PAGE_SIZE
    if do_read_all_items:
        limit = settings.DYNAMODB_MAX_PAGE_SIZE
    # Since we don't use boto3's automated pagination, set MaxItems to the same value as PageSize.
    #  See:
    #   https://jira.ci.motional.com/atljira/browse/BAS-2874
    #   https://github.com/boto/botocore/blob/develop/botocore/paginate.py#L252_L329
    config["MaxItems"] = limit
    config["PageSize"] = limit
    if starting_token:
        config["StartingToken"] = starting_token
    return config
