"""
** AWS LAMBDA CLIENT **
=======================

```py
import aws_lambda_client

client = aws_lambda_client.AwsLambdaClient()

payload = {
    "text": "Hello world from aws-lambda-client pytests!",
    "sender_app": "AWS_LAMBDA_CLIENT",
}
try:
    resp = self.client.invoke("botte-be-prod-message", payload=payload)
except aws_lambda_client.LambdaNotFound:
    ...
assert resp["StatusCode"] == 200

try:
    resp = self.client.get_url("botte-be-prod-endpoint-message")
except aws_lambda_client.LambdaNotFound:
    ...
assert resp[0] == "https://5t325uqwq7.execute-api.eu-south-1.amazonaws.com"
assert resp[1] == "/message"
assert resp[2] == "POST"
```
"""

import json
from typing import Any

import boto3
import json_utils

__all__ = [
    "AwsLambdaClient",
    "BaseAwsLambdaClientException",
    "LambdaNotFound",
]


class AwsLambdaClient:
    """
    Read and write params and secrets to AWS Param Store (part of AWS Systems Manager).

    Wrapper around boto3 client:
     https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html
    """

    def __init__(self) -> None:
        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.26.92/reference/services/lambda.html
        self.client = boto3.client("lambda")

    def invoke(self, lambda_name: str, payload: Any):
        """
        Invoke a Lambda with a payload.

        Args:
            lambda_name (string): eg. "botte-be-prod-endpoint-message" or
             "arn:aws:lambda:eu-south-1:477353422995:function:botte-be-prod-endpoint-message" or
             "477353422995:function:botte-be-prod-endpoint-message"
            payload: anything that can be converted to JSON.
        """
        payload = json_utils.to_json_string(payload)
        payload = payload.encode()
        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.26.92/reference/services/lambda/client/invoke.html
        try:
            response = self.client.invoke(FunctionName=lambda_name, Payload=payload)
        except self.client.exceptions.ResourceNotFoundException as exc:
            raise LambdaNotFound(lambda_name) from exc
        return response

    def get_url(self, lambda_name: str) -> tuple[str, str | None, str | None]:
        """
        Get the base HTTP url assigned to a Lambda either with a function URL or
         API Gateway V2.
        Used, mostly, in end-to-end tests.

        Args:
            lambda_name (string): eg. "botte-be-prod-endpoint-message" or
             "arn:aws:lambda:eu-south-1:477353422995:function:botte-be-prod-endpoint-message" or
             "477353422995:function:botte-be-prod-endpoint-message"

        Returns tuple(str, str | None, str | None): a tuple of 3 strings with API Gateway V2:
           url (eg. "https://5t325uqwq7.execute-api.eu-south-1.amazonaws.com")
           url path (eg. "/message")
           HTTP method (eg. "POST")
         And with function URL:
           base url (eg. "https://kvzlxwztgwjlardxdhiwgmcun40latmm.lambda-url.eu-south-1.on.aws/")
           url path=None
           HTTP method=None
        """
        # First, try to extract the Lambda function URL (without API Gateway).
        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.26.92/reference/services/lambda/client/get_function_url_config.html
        maybe_does_not_exist = False
        try:
            response = self.client.get_function_url_config(FunctionName=lambda_name)
            base_url = response.get("FunctionUrl")
            return base_url, None, None
        except self.client.exceptions.ResourceNotFoundException:
            maybe_does_not_exist = True

        # Second, if the first step failed, try to extract the API Gateway V2 url configured
        #  for this Lambda.
        # Inspect the Lambda to get the API Gateway V2.
        # Note: there is no way to extract the method ("POST") and the url path ("/message")
        #  from inspecting the Lambda with boto3.
        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.26.92/reference/services/lambda/client/get_policy.html
        try:
            response = self.client.get_policy(FunctionName=lambda_name)
        except self.client.exceptions.ResourceNotFoundException as exc:
            if maybe_does_not_exist:
                raise LambdaNotFound(lambda_name) from exc
            raise

        policy = response.get("Policy")
        if not policy:
            raise BaseAwsLambdaClientException(
                "Couldn't find any policy for this Lambda (there should be a"
                " policy to allow API Gateway V2 to invoke this Lambda"
            )
        policy = json.loads(policy)
        # policy example:
        # {
        #     "Version": "2012-10-17",
        #     "Id": "default",
        #     "Statement": [
        #         {
        #             "Sid": "botte-be-prod-EndpointDashmessageLambdaPermissionHttpApi-29mtVA5xngxw",
        #             "Effect": "Allow",
        #             "Principal": {"Service": "apigateway.amazonaws.com"},
        #             "Action": "lambda:InvokeFunction",
        #             "Resource": "arn:aws:lambda:eu-south-1:477353422995:function:botte-be-prod-endpoint-message",
        #             "Condition": {
        #                 "ArnLike": {
        #                     "AWS:SourceArn": "arn:aws:execute-api:eu-south-1:477353422995:5t325uqwq7/*"
        #                 }
        #             },
        #         }
        #     ],
        # }

        source_arn = None
        # Get the policy that allows API Gateway V2 to trigger this Lambda.
        for stm in policy.get("Statement", []):
            if "apigateway" in stm.get("Principal", {}).get("Service", ""):
                condition = stm.get("Condition", {})
                source_arn = condition.get("ArnLike", {}).get("AWS:SourceArn")
        if not source_arn:
            raise BaseAwsLambdaClientException(
                "Could find a policy with Principal>Service=apigateway "
                " and a proper Condition>ArnLike>AWS:SourceArn to allow "
                " API Gateway V2 to invoke this Lambda"
            )

        # Extract the API Gateway V2 id from that policy.
        # Eg. source_arn = "arn:aws:execute-api:eu-south-1:477353422995:5t325uqwq7/*".
        # From the source_arn string we need to extract the API Gateway V2 id: "5t325uqwq7".
        api_gateway_id = source_arn[source_arn.rfind(":") + 1 :].replace("/*", "")
        if not api_gateway_id:
            raise BaseAwsLambdaClientException(
                "Couldn't find the API Gateway V2 id in the Lambda policy"
                " that should allow it to invoke this Lambda"
            )

        # Inspect the API Gateway V2 to extract the base url
        #  ("https://5t325uqwq7.execute-api.eu-south-1.amazonaws.com"),
        #  the method ("POST") and the url path ("/message").
        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.26.92/reference/services/apigatewayv2.html
        apigw_client = boto3.client("apigatewayv2")
        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.26.92/reference/services/apigatewayv2/client/get_api.html
        response = apigw_client.get_api(ApiId=api_gateway_id)
        base_url = response.get("ApiEndpoint")

        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.26.92/reference/services/apigatewayv2/client/get_integrations.html
        response = apigw_client.get_integrations(ApiId=api_gateway_id)
        # response example:
        # {
        #     "Items": [
        #         {
        #             "ConnectionType": "INTERNET",
        #             "IntegrationId": "jwsq0jf",
        #             "IntegrationMethod": "POST",
        #             "IntegrationType": "AWS_PROXY",
        #             "IntegrationUri": "arn:aws:lambda:eu-south-1:477353422995:function:botte-be-prod-endpoint-message",
        #             "PayloadFormatVersion": "2.0",
        #             "TimeoutInMillis": 30000,
        #         }
        #     ],
        #     ...
        # }
        integration_id = None
        for item in response.get("Items", []):
            if item.get("IntegrationUri", "").endswith(lambda_name):
                integration_id = item.get("IntegrationId")
        if not integration_id:
            raise BaseAwsLambdaClientException(
                f"Couldn't find the IntegrationId named {lambda_name} in the "
                " API Gateway V2 id={api_gateway_id}"
            )

        # Docs: https://boto3.amazonaws.com/v1/documentation/api/1.26.92/reference/services/apigatewayv2/client/get_routes.html
        response = apigw_client.get_routes(ApiId=api_gateway_id)
        route_key = None
        for item in response.get("Items", []):
            # An item is like:
            # {'ApiKeyRequired': False,
            #  'AuthorizationScopes': [],
            #  'AuthorizationType': 'CUSTOM',
            #  'AuthorizerId': '5msjto',
            #  'RequestModels': {},
            #  'RouteId': '426oouh',
            #  'RouteKey': 'POST /message',
            #  'Target': 'integrations/jwsq0jf'}
            if item.get("Target", "").endswith(integration_id):
                route_key = item.get("RouteKey", "")
        if not route_key:
            raise BaseAwsLambdaClientException(
                f"Couldn't find the RouteKey amongst all routes with "
                f" {integration_id} in Target"
            )
        http_method, url_path = route_key.split(" ")

        return base_url, url_path, http_method


class BaseAwsLambdaClientException(Exception):
    pass


class LambdaNotFound(BaseAwsLambdaClientException):
    def __init__(self, lambda_name):
        self.lambda_name = lambda_name
