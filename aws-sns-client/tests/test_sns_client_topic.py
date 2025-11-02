from unittest import mock

import pytest

import aws_sns_client


class TestSnsTopic:
    def setup_method(self):
        self.arn = "arn:aws:sns:eu-south-1:477353422995:aws-watchdog-errors-prod"
        self.sns_client = aws_sns_client.SnsTopic(self.arn)
        self.body = dict(error="Ooooh error!")

    def test_happy_flow(self):
        response = self.sns_client.publish(self.body)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

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
            sns_client = aws_sns_client.SnsTopic(self.arn)
            with pytest.raises(aws_sns_client.BotoAuthError):
                sns_client.publish(self.body)

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
            sns_client = aws_sns_client.SnsTopic(self.arn)
            with pytest.raises(aws_sns_client.BotoAuthError):
                sns_client.publish(self.body)

    def test_no_aws_profile(self, monkeypatch):
        monkeypatch.setenv("AWS_PROFILE", "XXXXXXXXXXXXX")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
        with pytest.raises(aws_sns_client.BotoAuthError):
            aws_sns_client.SnsTopic(self.arn)

    def test_invalid_aws_session_token(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "xxx")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "xxx")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "xxx")
        sns_client = aws_sns_client.SnsTopic(self.arn)
        with pytest.raises(aws_sns_client.BotoAuthError) as exc:
            sns_client.publish(self.body)
        assert "InvalidClientTokenId" in str(exc.value.__cause__)

    # def test_expired_aws_session_token(self, monkeypatch):
    #     # You need to type in your actual key id and access to record this vcr episode. Then replace them with xxx.
    #     monkeypatch.setenv("AWS_ACCESS_KEY_ID", "xxx")
    #     monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "xxx")
    #     monkeypatch.setenv("AWS_SESSION_TOKEN", "xxx")
    #     sns_client = aws_sns_client.SnsTopic(self.arn)
    #     with pytest.raises(aws_sns_client.BotoAuthErrorTokenExpired):
    #         sns_client.publish(self.body)

    def test_topic_not_found(self):
        arn = self.arn + "XXX"
        sns_client = aws_sns_client.SnsTopic(arn)
        with pytest.raises(aws_sns_client.TopicNotFound):
            sns_client.publish(self.body)

    def test_topic_not_in_region(self):
        arn = self.arn.replace("eu-south-1", "ap-southeast-1")
        sns_client = aws_sns_client.SnsTopic(arn)
        with pytest.raises(aws_sns_client.TopicNotFound):
            sns_client.publish(self.body)

    def test_region_does_not_exist(self):
        arn = self.arn.replace("eu-south-1", "us-east-1XXX")
        sns_client = aws_sns_client.SnsTopic(arn)
        with pytest.raises(aws_sns_client.TopicNotFound):
            sns_client.publish(self.body)

    def test_json_body_string(self):
        body = '{"color": "red"}'
        response = self.sns_client.publish(body, do_set_json_content_type=True)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_json_body_string_invalid(self):
        body = '{"color'
        response = self.sns_client.publish(body, do_set_json_content_type=True)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_json_body_dict(self):
        body = dict(color="red")
        response = self.sns_client.publish(body, do_set_json_content_type=True)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_json_body_dict_non_serializable(self):
        body = dict(
            color="red", myclass=aws_sns_client.SnsTopic
        )  # SnsTopic is not JSON serializable.
        with pytest.raises(aws_sns_client.NotJsonSerializable):
            self.sns_client.publish(body, do_set_json_content_type=True)

    def test_non_json_body_string(self):
        body = "Hello"
        response = self.sns_client.publish(body, do_set_json_content_type=False)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_non_json_body_dict(self):
        body = dict(color="red")
        with pytest.raises(aws_sns_client.InvalidPublishMethodParameter):
            self.sns_client.publish(body, do_set_json_content_type=False)
