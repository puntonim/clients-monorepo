import abc
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Any, Optional

from botocore import exceptions as botocore_exceptions
from botocore.paginate import PageIterator
from ksuid import KsuidMs

from ..conf import settings
from ..utils.log_utils import logger
from . import dynamodb_utils_exceptions as exceptions

# TODO consider making a dynamodb_client lib out of this module.


class ReturnValuesEnum(Enum):
    NONE = "NONE"
    ALL_OLD = "ALL_OLD"
    UPDATED_OLD = "UPDATED_OLD"
    ALL_NEW = "ALL_NEW"
    UPDATED_NEW = "UPDATED_NEW"


class UpdateExpressionFactory:
    """
    Make an expression to be used with update_item() or transact_write_items()
     to create or update an item in DynamoDB.

    Notes:
        - Do use `do_skip_if_existing` to update an attribute only if it doesn't exist, example:
            factory.add_attr("region", region, do_skip_if_existing=True)
        - You can add index alongside their bound attribute, example:
            factory.add_attr("region", region, dict(gsi_session_pk_region=region))
          Or as regular attributes:
            factory.add_attr("gsi_session_pk_region", region)

    Examples:
        # With update_item():
        factory = UpdateExpressionFactory("pk1", "sk1")
        region = "sg-one-north"
        factory.add_attr("region", region, dict(gsi_session_pk_region=region), do_skip_if_existing=True)
        update_item_exp = factory.make()
        response: UpdateItemOutputTableTypeDef = get_table_resource().update_item(**update_item_exp)

        # With transact_write_items():
        factory = UpdateExpressionFactory("pk1", "sk1")
        region = "sg-one-north"
        factory.add_attr("region", region, dict(gsi_session_pk_region=region))
        transact_item = factory.make(is_transaction=True, table_name=...)
        response: TransactWriteItemTypeDef = get_table_resource().meta.client.transact_write_items(
            # Note: TransactItems accepts only list, not tuple.
            TransactItems=[transact_item]
        )

    Returns [dict]:
        # If not is_transaction:
        {
            "Key": {
                "PK": "pk1",
                "SK": "sk1",
            },
            "UpdateExpression": "
                SET #region = if_not_exists(#region, :region),
                #drivelog = :drivelog,
                #gsi_session_pk_region = if_not_exists(#gsi_session_pk_region, :gsi_session_pk_region)
            ",

            "ExpressionAttributeNames": {
                "#region": "Region",
                "#drivelog": "Drivelog",
                "#gsi_session_pk_region": "GSISessionPKRegion",
            }

            "ExpressionAttributeValues": {
                ":region": "sg-one-north",
                ":drivelog": "2022.06.23.16.51.09_g2h-veh-8006",
                ":gsi_session_pk_region": "sg-one-north",
            },

            "ReturnValues": "NONE",

            # Comment this out because the Dataset might already exist (update case).
            # ConditionExpression="attribute_exists(PK)",
        }

        # If is_transaction:
        {
            "Update": {
                "TableName": "MyTable",

                ↑↑ SAME DICT ITEMS AS CASE NO TRANSACTION (see above) ↑↑

                "ReturnValuesOnConditionCheckFailure": "NONE",
            }
        }
    """

    def __init__(self, pk: str, sk: str):
        self.pk = pk
        self.sk = sk
        self.to_update_if_not_exists = dict()
        self.to_update_if_not_exists_ix = dict()
        self.to_update = dict()
        self.to_update_ix = dict()

    def _add_attr_to_update_if_not_exists(
        self, attr_name: str, attr_value: Any, indexes: Optional[dict] = None
    ):
        self.to_update_if_not_exists[attr_name] = attr_value
        if indexes:
            if None in indexes.values():
                raise exceptions.IndexValueNoneError("Index values cannot be None")
            self.to_update_if_not_exists_ix.update(indexes)

    def _add_attr_to_update(
        self, attr_name: str, attr_value: Any, indexes: Optional[dict] = None
    ):
        self.to_update[attr_name] = attr_value
        if indexes:
            if None in indexes.values():
                raise exceptions.IndexValueNoneError("Index values cannot be None")
            self.to_update_ix.update(indexes)

    def add_attr(
        self,
        attr_name: str,
        attr_value: Any,
        indexes: Optional[dict] = None,
        do_skip_if_existing=True,
    ):
        """
        Add an attribute to be updated.
        Notice that you can add attrs and bound indexes invoking this method with the
         `indexes` arg. But you can also add indexes as if they were regular attributes
         by invoking this method with `attr_name` and `attr_value` that are the actual
         indexed attrs (and indeed no `indexes`).

        Args:
            attr_name: name of the attribute to update.
            attr_value: value of the attribute to update.
            indexes: dict of indexes that use the attribute to update.
                The key is the name of the indexed attribute to update.
                The value is the value of the indexed attribute to update.
                Value cannot be None.
            do_skip_if_existing: if True the update expression will include
                `if_not_exists` so that the attribute is updated only if
                there is no existing item (so it's a create operation) or
                if the existing item does not have the attribute.
        """
        if do_skip_if_existing:
            self._add_attr_to_update_if_not_exists(attr_name, attr_value, indexes)
        else:
            self._add_attr_to_update(attr_name, attr_value, indexes)

    def make(
        self,
        is_transaction=False,
        table_name: Optional[str] = None,
        return_values=ReturnValuesEnum.NONE,
    ) -> dict:
        if not self.to_update_if_not_exists and not self.to_update:
            raise exceptions.NoAttrsAdded("`add_attr()` must be invoked at least once")

        # Ensure `table_name` and `is_transaction` are True together.
        if is_transaction and not table_name:
            raise ValueError(
                "`table_name` cannot be None when `is_transaction` is True"
            )
        if not is_transaction and table_name:
            raise ValueError("`table_name` must be None when `is_transaction` is True")

        # Ensure `return_values` is NONE when `is_transaction` is True since `return_values` are not supported
        #  for transactions.
        if is_transaction and return_values != ReturnValuesEnum.NONE:
            raise ValueError(
                "`return_values` must be ReturnValuesEnum.None when `is_transaction` is True"
            )

        update_exp = ""
        exp_attr_values = dict()

        # Add all `to_update_if_not_exists` attrs.
        # Note: the attribute could also be an index if it was added
        #  not with `add_attr(..., indexes)` but instead as a regular attribute
        #  with `add_attr(attr_name, attr_value)`.
        for attr_name, attr_value in self.to_update_if_not_exists.items():
            # If this same attr is in `to_updated` then skip it now, but
            #  if we got 2 diff values, then raise.
            if attr_name in self.to_update:
                if attr_value != (_ := self.to_update[attr_name]):
                    raise exceptions.AttributeValuesConflict(
                        f"Different values provided for {attr_name}: {attr_value} and {_}"
                    )
                continue
            s = f", #{attr_name} = if_not_exists(#{attr_name}, :{attr_name})"
            update_exp += s
            exp_attr_values[f":{attr_name}"] = attr_value

        # Add all attrs `to_update`.
        # Note: the attribute could also be an index if it was added
        #  not with `add_attr(..., indexes)` but instead as a regular attribute
        #  with `add_attr(attr_name, attr_value)`.
        for attr_name, attr_value in self.to_update.items():
            update_exp += f", #{attr_name} = :{attr_name}"
            exp_attr_values[f":{attr_name}"] = attr_value

        # Add all indexes `to_update_if_not_exists_ix`.
        for attr_name, attr_value in self.to_update_if_not_exists_ix.items():
            # If this same attr must to_be_updated then skip it now, but
            #  if we got 2 diff values, then raise.
            if attr_name in self.to_update_ix:
                if attr_value != (_ := self.to_update_ix[attr_name]):
                    raise exceptions.AttributeValuesConflict(
                        f"Different values provided for {attr_name}: {attr_value} and {_}"
                    )
                continue
            s = f", #{attr_name} = if_not_exists(#{attr_name}, :{attr_name})"
            update_exp += s
            exp_attr_values[f":{attr_name}"] = attr_value

        # Add all indexes `to_update_ix`.
        for attr_name, attr_value in self.to_update_ix.items():
            update_exp += f", #{attr_name} = :{attr_name}"
            exp_attr_values[f":{attr_name}"] = attr_value

        # Fix the beginning of `update_exp` because right now it starts with ", ".
        update_exp = "SET" + update_exp[1:]

        # Add all attr names.
        exp_attr_names = dict()
        for attr_name in {
            *self.to_update_if_not_exists.keys(),
            *self.to_update_if_not_exists_ix.keys(),
            *self.to_update.keys(),
            *self.to_update_ix.keys(),
        }:
            exp_attr_names[f"#{attr_name}"] = dynamize(attr_name)

        expression = {
            "Key": {
                "PK": self.pk,
                "SK": self.sk,
            },
            # Comment this out to support the update.
            # ConditionExpression="attribute_exists(PK)",
            "UpdateExpression": update_exp,
            "ExpressionAttributeNames": exp_attr_names,
            "ExpressionAttributeValues": exp_attr_values,
        }
        if is_transaction:
            expression = {"Update": {"TableName": table_name, **expression}}
            expression["Update"]["ReturnValuesOnConditionCheckFailure"] = (
                ReturnValuesEnum.NONE.value
            )
        else:
            expression["ReturnValues"] = return_values.value
        return expression


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


def serialize_to_dynamodb(obj) -> Optional[str]:
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, KsuidMs) or isinstance(obj, str):
        return str(obj)
    return obj


def get_pagination_config(
    do_read_all_items=False,
    starting_token: Optional[str] = None,
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


class PaginatedResponse(abc.ABC):
    def __init__(self, response_iterator: PageIterator):
        try:
            self.full_result = response_iterator.build_full_result()
        except botocore_exceptions.ParamValidationError as exc:
            if "ExclusiveStartKey" in str(exc):
                logger.exception("Dynamodb starting token pagination error")
                raise exceptions.StartingTokenPaginationConfigError from exc
            logger.exception("Dynamodb Pagination error")
            raise exceptions.PaginationConfigError from exc

    @property
    @abc.abstractmethod
    def items(self) -> Any:
        pass

    @property
    def next_token(self) -> Optional[str]:
        return self.full_result.get("NextToken")
