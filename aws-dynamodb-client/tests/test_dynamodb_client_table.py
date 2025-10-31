from datetime import datetime
from unittest import mock

import pytest

import aws_dynamodb_client


class TestDynamodbTableWrite:
    def setup_method(self):
        self.name = "botte-be-task-prod"
        self.item = {
            "PK": "BOTTE_MESSAGE",
            "SK": "2XxEn9LlUFuTyn0tOCySn11smMS",  # ksuid.KsuidMs().
            "TaskId": "BOTTE_MESSAGE",
            "SenderService": "DYNAMODB_CLIENT_PYTEST",
            "ExpirationTs": 1699565417,
            "Payload": {
                "text": "Hello world from (clients-monorepo) DynamoDB Client Pytest"
            },
        }

    def test_happy_flow(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        response = table.write(self.item)
        assert response == {
            "ResponseMetadata": {
                "RequestId": "SSAOU4PD9U9NKQ7PVEDBKCJ7KVVV4KQNSO5AEMVJF66Q9ASUAAJG",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "connection": "keep-alive",
                    "content-length": "2",
                    "content-type": "application/x-amz-json-1.0",
                    "date": "Fri, 31 Oct 2025 18:53:59 GMT",
                    "server": "Server",
                    "x-amz-crc32": "2745614147",
                    "x-amzn-requestid": "SSAOU4PD9U9NKQ7PVEDBKCJ7KVVV4KQNSO5AEMVJF66Q9ASUAAJG",
                },
                "RetryAttempts": 0,
            }
        }

    def test_no_local_aws_auth(self, monkeypatch):
        # Boto3 last attempted auth is "Instance metadata service on an Amazon EC2
        #  instance that has an IAM role configured", see:
        #  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        # Which makes a PUT request to the local address:
        #  http://169.254.169.254/latest/api/token
        # Which vcrpy fails at recording because in a dev machine that IP is unknown.
        # So monkeypatching is the simplest way to make that auth fail.
        with mock.patch(
            "botocore.utils.InstanceMetadataFetcher.retrieve_iam_role_credentials",
            return_value={},
        ):
            monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "XXXX")
            monkeypatch.setenv("AWS_CONFIG_FILE", "XXXX")
            monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
            monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
            monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
            table = aws_dynamodb_client.DynamodbTable(self.name)
            with pytest.raises(aws_dynamodb_client.BotoAuthError):
                table.write(self.item)

    def test_no_creds_in_local_aws_file(self, monkeypatch):
        # Boto3 last attempted auth is "Instance metadata service on an Amazon EC2
        #  instance that has an IAM role configured", see:
        #  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        # Which makes a PUT request to the local address:
        #  http://169.254.169.254/latest/api/token
        # Which vcrpy fails at recording because in a dev machine that IP is unknown.
        # So monkeypatching is the simplest way to make that auth fail.
        with mock.patch(
            "botocore.utils.InstanceMetadataFetcher.retrieve_iam_role_credentials",
            return_value={},
        ):
            monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
            monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")
            monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
            monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
            monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
            table = aws_dynamodb_client.DynamodbTable(self.name)
            with pytest.raises(aws_dynamodb_client.BotoAuthError):
                table.write(self.item)

    def test_no_aws_profile(self, monkeypatch):
        monkeypatch.setenv("AWS_PROFILE", "XXXXXXXXXXXXX")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
        with pytest.raises(aws_dynamodb_client.BotoAuthError):
            aws_dynamodb_client.DynamodbTable(self.name)

    def test_invalid_aws_session_token(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "xxx")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "xxx")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "xxx")
        table = aws_dynamodb_client.DynamodbTable(self.name)
        with pytest.raises(aws_dynamodb_client.BotoAuthError) as exc:
            table.write(self.item)
        assert "UnrecognizedClientException" in str(exc.value.__cause__)

    # def test_expired_aws_session_token(self, monkeypatch):
    #     # You need to TEMPORARLY type in your actual ACCESS_KEY_ID and SECRET_ACCESS_KEY
    #     #  to record this vcr episode. Then replace them with xxx.
    #     monkeypatch.setenv("AWS_ACCESS_KEY_ID", "XXX")
    #     monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "XXX")
    #     monkeypatch.setenv("AWS_SESSION_TOKEN", "xxx")
    #     table = aws_dynamodb_client.DynamodbTable(self.name)
    #     with pytest.raises(aws_dynamodb_client.BotoAuthErrorTokenExpired):
    #         table.write(self.item)

    # # Note: it cannot record the cassette.
    # def test_invalid_aws_region_name(self):
    #     table = aws_dynamodb_client.DynamodbTable(aws_region_name="XXX", name=self.name)
    #     with pytest.raises(aws_dynamodb_client.EndpointConnectionError):
    #         table.write(self.item)

    def test_table_name_not_found(self):
        name = "XXX"
        table = aws_dynamodb_client.DynamodbTable(name)
        with pytest.raises(aws_dynamodb_client.TableDoesNotExist):
            table.write(self.item)

    def test_item_non_dict(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        with pytest.raises(aws_dynamodb_client.InvalidPutItemMethodParameter):
            table.write(item=lambda x: "XXX")

    def test_item_non_json_serializable(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        self.item["TaskId"] = datetime(2023, 10, 10)
        with pytest.raises(aws_dynamodb_client.InvalidPutItemMethodParameter):
            table.write(self.item)

    def test_item_empty_dict(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        self.item = dict()
        with pytest.raises(aws_dynamodb_client.InvalidPutItemMethodParameter):
            table.write(self.item)

    def test_item_none(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        self.item = None
        with pytest.raises(aws_dynamodb_client.InvalidPutItemMethodParameter):
            table.write(self.item)

    def test_duplicated_item_and_do_not_override_existing(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        table.write(self.item)

        with pytest.raises(aws_dynamodb_client.PrimaryKeyConstraintError):
            table.write(self.item)

    def test_duplicated_item_and_override_existing(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        table.write(self.item)

        table.write(self.item, do_overwrite_existing=True)


class TestDynamodbTableReadByPk:
    def setup_method(self):
        self.name = "botte-be-task-prod"
        self.item = {
            "PK": "BOTTE_MESSAGE",
            "SK": "2XxEn9LlUFuTyn0tOCySn11smMS",  # ksuid.KsuidMs().
            "TaskId": "BOTTE_MESSAGE",
            "SenderService": "DYNAMODB_CLIENT_PYTEST",
            "ExpirationTs": 1762020871,
            "Payload": {
                "text": "Hello world from (clients-monorepo) DynamoDB Client Pytest"
            },
        }

    def test_happy_flow(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        response = table.read_by_pk(self.item["PK"])
        assert response["Items"][0] == self.item
        assert response["Count"] == 1
        assert response == {
            "Items": [self.item],
            "Count": 1,
            "ScannedCount": 1,
            "ResponseMetadata": {
                "RequestId": "9EAD1I8US34CUHE78ICT820P9BVV4KQNSO5AEMVJF66Q9ASUAAJG",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "connection": "keep-alive",
                    "content-length": "312",
                    "content-type": "application/x-amz-json-1.0",
                    "date": "Fri, 31 Oct 2025 19:15:35 GMT",
                    "server": "Server",
                    "x-amz-crc32": "2687463111",
                    "x-amzn-requestid": "9EAD1I8US34CUHE78ICT820P9BVV4KQNSO5AEMVJF66Q9ASUAAJG",
                },
                "RetryAttempts": 0,
            },
        }

    def test_no_local_aws_auth(self, monkeypatch):
        # Boto3 last attempted auth is "Instance metadata service on an Amazon EC2
        #  instance that has an IAM role configured", see:
        #  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        # Which makes a PUT request to the local address:
        #  http://169.254.169.254/latest/api/token
        # Which vcrpy fails at recording because in a dev machine that IP is unknown.
        # So monkeypatching is the simplest way to make that auth fail.
        with mock.patch(
            "botocore.utils.InstanceMetadataFetcher.retrieve_iam_role_credentials",
            return_value={},
        ):
            monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "XXXX")
            monkeypatch.setenv("AWS_CONFIG_FILE", "XXXX")
            monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
            monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
            monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
            table = aws_dynamodb_client.DynamodbTable(self.name)
            with pytest.raises(aws_dynamodb_client.BotoAuthError):
                table.read_by_pk(self.item["PK"])

    def test_no_creds_in_local_aws_file(self, monkeypatch):
        # Boto3 last attempted auth is "Instance metadata service on an Amazon EC2
        #  instance that has an IAM role configured", see:
        #  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        # Which makes a PUT request to the local address:
        #  http://169.254.169.254/latest/api/token
        # Which vcrpy fails at recording because in a dev machine that IP is unknown.
        # So monkeypatching is the simplest way to make that auth fail.
        with mock.patch(
            "botocore.utils.InstanceMetadataFetcher.retrieve_iam_role_credentials",
            return_value={},
        ):
            monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
            monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")
            monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
            monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
            monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
            table = aws_dynamodb_client.DynamodbTable(self.name)
            with pytest.raises(aws_dynamodb_client.BotoAuthError):
                table.read_by_pk(self.item["PK"])

    def test_no_aws_profile(self, monkeypatch):
        monkeypatch.setenv("AWS_PROFILE", "XXXXXXXXXXXXX")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
        with pytest.raises(aws_dynamodb_client.BotoAuthError):
            aws_dynamodb_client.DynamodbTable(self.name)

    def test_invalid_aws_session_token(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "xxx")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "xxx")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "xxx")
        table = aws_dynamodb_client.DynamodbTable(self.name)
        with pytest.raises(aws_dynamodb_client.BotoAuthError) as exc:
            table.read_by_pk(self.item["PK"])
        assert "UnrecognizedClientException" in str(exc.value.__cause__)

    # def test_expired_aws_session_token(self, monkeypatch):
    #     # You need to TEMPORARLY type in your actual ACCESS_KEY_ID and SECRET_ACCESS_KEY
    #     #  to record this vcr episode. Then replace them with xxx.
    #     monkeypatch.setenv("AWS_ACCESS_KEY_ID", "XXX")
    #     monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "XXX")
    #     monkeypatch.setenv("AWS_SESSION_TOKEN", "xxx")
    #     table = aws_dynamodb_client.DynamodbTable(self.name)
    #     with pytest.raises(aws_dynamodb_client.BotoAuthErrorTokenExpired):
    #         table.read_by_pk(self.item["PK"])

    # # Note: it cannot record the cassette.
    # def test_invalid_aws_region_name(self):
    #     table = aws_dynamodb_client.DynamodbTable(aws_region_name="XXX", name=self.name)
    #     with pytest.raises(aws_dynamodb_client.EndpointConnectionError):
    #         table.read_by_pk(self.item["PK"])

    def test_table_name_not_found(self):
        name = "XXX"
        table = aws_dynamodb_client.DynamodbTable(name)
        with pytest.raises(aws_dynamodb_client.TableDoesNotExist):
            table.read_by_pk(self.item["PK"])

    def test_pk_attr_name_does_not_exist(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        with pytest.raises(aws_dynamodb_client.PkAttrNameInvalid):
            table.read_by_pk(self.item["PK"], pk_attr_name="XXX")

    def test_no_items(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        response = table.read_by_pk("XXX")
        assert response["Count"] == 0

    def test_pk_value_non_string(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        with pytest.raises(aws_dynamodb_client.PkAttrValueNonString):
            table.read_by_pk({"XXX": 123})


class TestDynamodbTableReadAll:
    def setup_method(self):
        self.name = "botte-be-task-prod"
        self.item = {
            "PK": "BOTTE_MESSAGE",
            "SK": "2XxEn9LlUFuTyn0tOCySn11smMS",  # ksuid.KsuidMs().
            "TaskId": "BOTTE_MESSAGE",
            "SenderService": "DYNAMODB_CLIENT_PYTEST",
            "ExpirationTs": 1762020871,
            "Payload": {
                "text": "Hello world from (clients-monorepo) DynamoDB Client Pytest"
            },
        }

    def test_happy_flow(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        response = table.read_all()
        assert response["Count"] == 1
        assert response["Items"][0] == self.item
        assert response == {
            "Items": [self.item],
            "Count": 1,
            "ScannedCount": 1,
            "ResponseMetadata": {
                "RequestId": "TT315IPDK2G7LM0522RTHR9L7JVV4KQNSO5AEMVJF66Q9ASUAAJG",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "content-type": "application/x-amz-json-1.0",
                    "x-amzn-requestid": "TT315IPDK2G7LM0522RTHR9L7JVV4KQNSO5AEMVJF66Q9ASUAAJG",
                    "content-length": "312",
                    "date": "Fri, 31 Oct 2025 20:04:48 GMT",
                    "server": "Server",
                    "connection": "keep-alive",
                    "x-amz-crc32": "2687463111",
                },
                "RetryAttempts": 0,
            },
        }

    def test_no_local_aws_auth(self, monkeypatch):
        # Boto3 last attempted auth is "Instance metadata service on an Amazon EC2
        #  instance that has an IAM role configured", see:
        #  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        # Which makes a PUT request to the local address:
        #  http://169.254.169.254/latest/api/token
        # Which vcrpy fails at recording because in a dev machine that IP is unknown.
        # So monkeypatching is the simplest way to make that auth fail.
        with mock.patch(
            "botocore.utils.InstanceMetadataFetcher.retrieve_iam_role_credentials",
            return_value={},
        ):
            monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "XXXX")
            monkeypatch.setenv("AWS_CONFIG_FILE", "XXXX")
            monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
            monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
            monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
            table = aws_dynamodb_client.DynamodbTable(self.name)
            with pytest.raises(aws_dynamodb_client.BotoAuthError):
                table.read_all()

    def test_no_creds_in_local_aws_file(self, monkeypatch):
        # Boto3 last attempted auth is "Instance metadata service on an Amazon EC2
        #  instance that has an IAM role configured", see:
        #  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        # Which makes a PUT request to the local address:
        #  http://169.254.169.254/latest/api/token
        # Which vcrpy fails at recording because in a dev machine that IP is unknown.
        # So monkeypatching is the simplest way to make that auth fail.
        with mock.patch(
            "botocore.utils.InstanceMetadataFetcher.retrieve_iam_role_credentials",
            return_value={},
        ):
            monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
            monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")
            monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
            monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
            monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
            table = aws_dynamodb_client.DynamodbTable(self.name)
            with pytest.raises(aws_dynamodb_client.BotoAuthError):
                table.read_all()

    def test_no_aws_profile(self, monkeypatch):
        monkeypatch.setenv("AWS_PROFILE", "XXXXXXXXXXXXX")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
        with pytest.raises(aws_dynamodb_client.BotoAuthError):
            aws_dynamodb_client.DynamodbTable(self.name)

    def test_invalid_aws_session_token(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "xxx")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "xxx")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "xxx")
        table = aws_dynamodb_client.DynamodbTable(self.name)
        with pytest.raises(aws_dynamodb_client.BotoAuthError) as exc:
            table.read_all()
        assert "UnrecognizedClientException" in str(exc.value.__cause__)

    # def test_expired_aws_session_token(self, monkeypatch):
    #     # You need to TEMPORARLY type in your actual ACCESS_KEY_ID and SECRET_ACCESS_KEY
    #     #  to record this vcr episode. Then replace them with xxx.
    #     monkeypatch.setenv("AWS_ACCESS_KEY_ID", "XXX")
    #     monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "XXX")
    #     monkeypatch.setenv("AWS_SESSION_TOKEN", "xxx")
    #     table = aws_dynamodb_client.DynamodbTable(self.name)
    #     with pytest.raises(aws_dynamodb_client.BotoAuthErrorTokenExpired):
    #         table.read_by_pk(self.item["PK"])

    # # Note: it cannot record the cassette.
    # def test_invalid_aws_region_name(self):
    #     table = aws_dynamodb_client.DynamodbTable(aws_region_name="XXX", name=self.name)
    #     with pytest.raises(aws_dynamodb_client.EndpointConnectionError):
    #         table.read_by_pk(self.item["PK"])

    def test_table_name_not_found(self):
        name = "XXX"
        table = aws_dynamodb_client.DynamodbTable(name)
        with pytest.raises(aws_dynamodb_client.TableDoesNotExist):
            table.read_by_pk(self.item["PK"])

    def test_no_items(self):
        table = aws_dynamodb_client.DynamodbTable(self.name)
        response = table.read_all()
        assert response["Count"] == 0
