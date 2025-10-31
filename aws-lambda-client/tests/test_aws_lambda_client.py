import pytest

import aws_lambda_client


class TestGetUrl:
    def setup_method(self):
        self.client = aws_lambda_client.AwsLambdaClient()

    def test_lambda_url(self):
        # Note: I temp edited the serverless config to make it use a function URL
        #  instead of API Gateway, just for the time of recording this VCR cassette,
        #  then reverted back to API Gateway.
        resp = self.client.get_url("botte-be-prod-endpoint-introspection")
        assert (
            resp[0]
            == "https://kvzlxwztgwjlardxdhiwgmcun40latmm.lambda-url.eu-south-1.on.aws/"
        )
        assert resp[1] is None
        assert resp[2] is None

    def test_lambda_doesnt_exists(self):
        with pytest.raises(aws_lambda_client.LambdaNotFound):
            self.client.get_url("XXX")

    def test_api_gateway_url(self):
        resp = self.client.get_url("botte-be-prod-endpoint-message")
        assert resp[0] == "https://5t325uqwq7.execute-api.eu-south-1.amazonaws.com"
        assert resp[1] == "/message"
        assert resp[2] == "POST"


class TestInvoke:
    def setup_method(self):
        self.client = aws_lambda_client.AwsLambdaClient()

    def test_happy_flow(self):
        payload = {
            "text": "Hello world from aws-lambda-client pytests!",
            "sender_app": "AWS_LAMBDA_CLIENT",
        }
        resp = self.client.invoke("botte-be-prod-message", payload=payload)
        assert resp["StatusCode"] == 200

    def test_lambda_doesnt_exists(self):
        with pytest.raises(aws_lambda_client.LambdaNotFound):
            self.client.invoke(
                "XXX", payload="Hello world from aws-lambda-client pytests!"
            )
