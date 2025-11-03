__all__ = [
    "BaseSnsTopicException",
    "TopicNotFound",
    "InvalidPublishMethodParameter",
    "NotJsonSerializable",
]


class BaseSnsTopicException(Exception):
    pass


class TopicNotFound(BaseSnsTopicException):
    def __init__(self, topic_arn: str):
        self.topic_arn = topic_arn


class InvalidPublishMethodParameter(BaseSnsTopicException):
    # Eg. Invalid parameter to the publish method, eg. Message is not a string
    # str(exc.__root__) contains a clear message.
    pass


class NotJsonSerializable(BaseSnsTopicException):
    pass
