__all__ = [
    "BaseDynamodbClientBaseException",
    "BaseErrorResponse",
    "BotoAuthError",
    "BotoAuthErrorTokenExpired",
    "EndpointConnectionError",
]


class BaseDynamodbClientBaseException(Exception):
    pass


class BaseErrorResponse(BaseDynamodbClientBaseException):
    message: str | None = (
        "Generic error response from DynamoDB; a common cause is expired/invalid aws_session_token"
    )

    def __init__(self, message=None):
        msg = self.message
        if message:
            msg = message
        if msg:
            super().__init__(msg)
        else:
            super().__init__()


class BotoAuthError(BaseErrorResponse):
    message: str | None = None


class BotoAuthErrorTokenExpired(BotoAuthError):
    message = "AWS token expired"


class EndpointConnectionError(BaseErrorResponse):
    message = "Endpoint not reachable, maybe the provided region does not exist or there is a network issue"
