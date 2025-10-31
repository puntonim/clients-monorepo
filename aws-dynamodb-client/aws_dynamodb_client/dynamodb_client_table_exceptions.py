__all__ = [
    "BaseDynamodbTableException",
    "TableDoesNotExist",
    "InvalidPutItemMethodParameter",
    "GenericBotoError",
    "PrimaryKeyConstraintError",
    "PkAttrNameInvalid",
    "PkAttrValueNonString",
]


class BaseDynamodbTableException(Exception):
    pass


class TableDoesNotExist(BaseDynamodbTableException):
    pass


class InvalidPutItemMethodParameter(BaseDynamodbTableException):
    # Eg. Invalid parameter to the put_item API method, eg. Item is not
    #  a dict. str(exc.__root__) contains a clear message.
    pass


class GenericBotoError(BaseDynamodbTableException):
    pass


class PrimaryKeyConstraintError(BaseDynamodbTableException):
    def __init__(self, item: dict):
        self.item = item


class PkAttrNameInvalid(BaseDynamodbTableException):
    def __init__(self, pk_attr_name: str):
        self.pk_attr_name = pk_attr_name


class PkAttrValueNonString(BaseDynamodbTableException):
    def __init__(self, pk_value: str):
        self.pk_value = pk_value
