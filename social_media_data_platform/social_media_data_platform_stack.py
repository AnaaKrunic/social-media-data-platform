from aws_cdk import (
    CfnOutput,
    Duration,
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as lambda_,
    aws_s3 as s3,
)
from constructs import Construct

class SocialMediaDataPlatformStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.raw_data_bucket = s3.Bucket(
            self,
            "RawDataBucket",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        # The function uses only the Python standard library and the boto3
        # version already provided by the Lambda Python runtime.
        self.hacker_news_ingestion = lambda_.Function(
            self,
            "HackerNewsIngestion",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda/ingestion"),
            timeout=Duration.minutes(10),
            memory_size=512,
            environment={
                "RAW_BUCKET_NAME": self.raw_data_bucket.bucket_name,
            },
        )

        # Least privilege: this function can create raw Hacker News files,
        # but cannot read, delete, or write to any other Bronze source.
        self.raw_data_bucket.grant_put(
            self.hacker_news_ingestion,
            "bronze/hackernews/*",
        )

        daily_ingestion_rule = events.Rule(
            self,
            "DailyHackerNewsIngestion",
            description="Collects the previous UTC day's Hacker News data into Bronze.",
            schedule=events.Schedule.cron(
                minute="0",
                hour="1",
            ),
        )

        daily_ingestion_rule.add_target(
            targets.LambdaFunction(
                self.hacker_news_ingestion,
                retry_attempts=2,
                max_event_age=Duration.hours(2),
            )
        )

        CfnOutput(
            self,
            "RawDataBucketName",
            value=self.raw_data_bucket.bucket_name,
            description="S3 bucket that holds the Bronze layer's raw data.",
        )
