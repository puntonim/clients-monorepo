from unittest import mock

import pytest

import aws_parameter_store_client
from aws_parameter_store_client.aws_parameter_store_client import cache


class TestPutParameter:
    def test_happy_flow(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        client.put_parameter(
            path="/test/aws/parameter/store/client",
            value="yesss!",
            do_overwrite=False,
        )

    def test_do_not_overwrite(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        with pytest.raises(aws_parameter_store_client.ParameterAlreadyExists):
            client.put_parameter(
                path="/test/aws/parameter/store/client",
                value="nooo!",
                do_overwrite=False,
            )

    def test_overwrite(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        client.put_parameter(
            path="/test/aws/parameter/store/client",
            value="nooo!",
            do_overwrite=True,
        )


class TestGetParameter:
    def setup_method(self):
        cache.clear_cache()

    def test_happy_flow(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        data = client.get_parameter(path="/test/aws/parameter/store/client")
        assert data == "nooo!"

    def test_does_not_exist(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        with pytest.raises(aws_parameter_store_client.ParameterNotFound):
            client.get_parameter(path="/test/aws/XXX")

    def test_cache(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        with mock.patch.object(
            client.client,
            "get_parameter",
            wraps=client.client.get_parameter,
        ) as mock_obj:
            data = client.get_parameter(
                path="/test/aws/parameter/store/client", cache_ttl=60
            )
            assert data == "nooo!"

            for _ in range(12):
                data = client.get_parameter(
                    path="/test/aws/parameter/store/client", cache_ttl=60
                )
                assert data == "nooo!"

        assert mock_obj.call_count == 1

    def test_cache_disabled(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        with mock.patch.object(
            client.client,
            "get_parameter",
            wraps=client.client.get_parameter,
        ) as mock_obj:
            data = client.get_parameter(
                path="/test/aws/parameter/store/client", cache_ttl=60
            )
            assert data == "nooo!"

            data = client.get_parameter(
                path="/test/aws/parameter/store/client",
                cache_ttl=60,
                do_skip_cache=False,
            )
            assert data == "nooo!"

            data = client.get_parameter(
                path="/test/aws/parameter/store/client",
                cache_ttl=60,
                do_skip_cache=True,
            )
            assert data == "nooo!"
            data = client.get_parameter(
                path="/test/aws/parameter/store/client",
                cache_ttl=60,
                do_skip_cache=True,
            )
            assert data == "nooo!"

        assert mock_obj.call_count == 3


class TestPutSecret:
    def test_happy_flow(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        client.put_secret(
            path="/test/aws/parameter/store/client/secret",
            value="yesss!",
            do_overwrite=False,
        )

    def test_do_not_overwrite(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        with pytest.raises(aws_parameter_store_client.ParameterAlreadyExists):
            client.put_secret(
                path="/test/aws/parameter/store/client/secret",
                value="nooo!",
                do_overwrite=False,
            )

    def test_overwrite(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        client.put_secret(
            path="/test/aws/parameter/store/client/secret",
            value="nooo!",
            do_overwrite=True,
        )


class TestGetSecret:
    def test_happy_flow(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        data = client.get_secret(path="/test/aws/parameter/store/client/secret")
        assert data == "nooo!"

    def test_does_not_exist(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        with pytest.raises(aws_parameter_store_client.ParameterNotFound):
            client.get_secret(path="/test/aws/XXX")
