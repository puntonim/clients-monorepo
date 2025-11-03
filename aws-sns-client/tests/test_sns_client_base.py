import aws_sns_client


class TestCreateTopic:
    def test_happy_flow(self):
        response = aws_sns_client.create_topic("aws-watchdog-pytest-topic")
        assert (
            response == "arn:aws:sns:eu-south-1:477353422995:aws-watchdog-pytest-topic"
        )
