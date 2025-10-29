"""
** AWS PARAMETER STORE CLIENT **
================================

```py
import aws_parameter_store_client

client = aws_parameter_store_client.AwsParameterStoreClient()
try:
    value = client.get_parameter(path="/my/parameter", cache_ttl=60)
except aws_parameter_store_client.ParameterNotFound as exc:
    ...
assert value == "thisismyvalue"
```
"""

import boto3
import cache_utils

__all__ = [
    "AwsParameterStoreClient",
    "ParameterAlreadyExists",
    "ParameterNotFound",
]


# Using a cache (for time) as global var, so shared across all AwsParameterStoreClient
#  instances in the same process.
cache = cache_utils.CacheForTimeMap()


class AwsParameterStoreClient:
    """
    Read and write params and secrets to AWS Param Store (part of AWS Systems Manager).

    Wrapper around boto3 client:
     https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html
    """

    def __init__(self) -> None:
        # Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html
        self.client = boto3.client("ssm")

    def _get(
        self, path: str, do_decrypt=False, cache_ttl=60, do_skip_cache=False
    ) -> str:
        # First check the cache.
        if not do_skip_cache:
            try:
                return cache.get(path)
            except (cache_utils.ItemExpired, cache_utils.KeyNotFound):
                # Cache miss.
                pass
        # Cache miss, so get it from Param Store.
        kwargs = dict(Name=path)
        if do_decrypt:
            kwargs["WithDecryption"] = do_decrypt
        try:
            parameter = self.client.get_parameter(**kwargs)
        except self.client.exceptions.ParameterNotFound as exc:
            raise ParameterNotFound(path) from exc
        value = parameter["Parameter"]["Value"]

        # cache the value.
        if not do_skip_cache:
            cache.set(path, value, cache_ttl)

        return value

    def get_parameter(self, path: str, cache_ttl=60, do_skip_cache=False) -> str:
        """
        Get a parameter stored in AWS Parameter Store.

        Args:
            path (str): parameter path.
            cache_ttl (int): time-to-live for the parameters in the in-memory cache, seconds.
            do_skip_cache (bool): True to skip the in-memory cache.
        """
        return self._get(
            path, do_decrypt=False, cache_ttl=cache_ttl, do_skip_cache=do_skip_cache
        )

    def get_secret(self, path: str, cache_ttl=60, do_skip_cache=False) -> str:
        """
        Get and decrypt a secret stored in AWS Parameter Store.

        Args:
            path (str): secret path.
            cache_ttl (int): time-to-live for the secret in the in-memory cache, seconds.
            do_skip_cache (bool): True to skip the in-memory cache.
        """
        return self._get(
            path, do_decrypt=True, cache_ttl=cache_ttl, do_skip_cache=do_skip_cache
        )

    def _put(
        self, path: str, value: str, is_secure_string=False, do_overwrite=False
    ) -> None:
        try:
            self.client.put_parameter(
                Name=path,
                Description="string",
                Value=value,
                Type="SecureString" if is_secure_string else "String",
                Overwrite=do_overwrite,
            )
        except self.client.exceptions.ParameterAlreadyExists as exc:
            raise ParameterAlreadyExists(path) from exc

    def put_parameter(self, path: str, value: str, do_overwrite=False) -> None:
        """
        Set a parameter in AWS Parameter Store.

        Args:
            path (str): parameter path.
            value: parameter value.
            do_overwrite (bool): True to overwrite, if a param already exists with the same path.
        """
        return self._put(path, value, is_secure_string=False, do_overwrite=do_overwrite)

    def put_secret(self, path: str, value: str, do_overwrite=False) -> None:
        """
        Set a secret in AWS Parameter Store.

        Args:
            path (str): secret path.
            value: secret value in plain-text (it will be encrypted by AWS).
            do_overwrite (bool): True to overwrite, if a secret already exists with the same path.
        """
        return self._put(path, value, is_secure_string=True, do_overwrite=do_overwrite)


class BaseAwsParameterStoreClientException(Exception):
    pass


class ParameterAlreadyExists(BaseAwsParameterStoreClientException):
    def __init__(self, path):
        self.path = path


class ParameterNotFound(BaseAwsParameterStoreClientException):
    def __init__(self, path):
        self.path = path
