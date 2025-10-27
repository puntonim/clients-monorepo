import pytest

import aws_parameter_store_client


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
    def test_happy_flow(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        data = client.get_parameter(path="/test/aws/parameter/store/client")
        assert data == "nooo!"

    def test_does_not_exist(self):
        client = aws_parameter_store_client.AwsParameterStoreClient()
        with pytest.raises(aws_parameter_store_client.ParameterNotFound):
            client.get_parameter(path="/test/aws/XXX")


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
