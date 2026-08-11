from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as lambda_,
    aws_s3 as s3,
)
from constructs import Construct

AWS_SDK_PANDAS_LAYER_VERSION = 13


class SocialMediaDataPlatformStack(Stack):
    """Medalion pipeline — Bronze + Silver on AWS Free Tier.

    Designed to stay within typical 12-month free limits when using:
      - Bitcoin_tweets_aws.csv (~80k rows, ~20 MB) from prepare_bitcoin_sample.py
      - HN_MAX_PAGES=5 per content type per day (~25k items/day max)
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.raw_data_bucket = s3.Bucket(
            self,
            "RawDataBucket",
            versioned=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
        )

        aws_sdk_pandas_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            "AwsSdkPandasLayer",
            f"arn:aws:lambda:{self.region}:336392948345:layer:AWSSDKPandas-Python312:{AWS_SDK_PANDAS_LAYER_VERSION}",
        )

        # --- Lambda 1: Hacker News Bronze ingestion ---
        self.hacker_news_ingestion = lambda_.Function(
            self,
            "HackerNewsIngestion",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda/ingestion"),
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "RAW_BUCKET_NAME": self.raw_data_bucket.bucket_name,
                "HN_MAX_PAGES": "5",
            },
        )

        self.raw_data_bucket.grant_put(
            self.hacker_news_ingestion,
            "bronze/hackernews/*",
        )

        events.Rule(
            self,
            "DailyHackerNewsIngestion",
            description="Daily HN Bronze ingestion (previous UTC day).",
            schedule=events.Schedule.cron(minute="0", hour="1"),
            targets=[
                targets.LambdaFunction(
                    self.hacker_news_ingestion,
                    retry_attempts=1,
                    max_event_age=Duration.hours(1),
                )
            ],
        )

        # --- Lambda 2: Silver normalization ---
        self.normalization = lambda_.Function(
            self,
            "SilverNormalization",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda/normalization"),
            timeout=Duration.minutes(10),
            memory_size=1536,
            layers=[aws_sdk_pandas_layer],
            environment={
                "DATA_BUCKET_NAME": self.raw_data_bucket.bucket_name,
            },
        )

        self.raw_data_bucket.grant_read(self.normalization, "bronze/*")
        self.raw_data_bucket.grant_read_write(self.normalization, "silver/*")

        events.Rule(
            self,
            "DailySilverNormalization",
            description="Daily Silver normalization for yesterday's HN data.",
            schedule=events.Schedule.cron(minute="30", hour="2"),
            targets=[
                targets.LambdaFunction(
                    self.normalization,
                    event=events.RuleTargetInput.from_object({"source": "hackernews"}),
                    retry_attempts=1,
                    max_event_age=Duration.hours(1),
                )
            ],
        )

        # --- Lambda 3: Gold transformation ---
        self.gold_transform = lambda_.Function(
            self,
            "GoldTransformation",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda/gold"),
            timeout=Duration.minutes(5),
            memory_size=1024,
            layers=[aws_sdk_pandas_layer],
            environment={
                "DATA_BUCKET_NAME": self.raw_data_bucket.bucket_name,
            },
        )

        self.raw_data_bucket.grant_read(self.gold_transform, "silver/*")
        self.raw_data_bucket.grant_read_write(self.gold_transform, "gold/*")

        events.Rule(
            self,
            "DailyGoldTransformation",
            description="Daily Gold metrics for yesterday's Silver data.",
            schedule=events.Schedule.cron(minute="0", hour="3"),
            targets=[
                targets.LambdaFunction(
                    self.gold_transform,
                    retry_attempts=1,
                    max_event_age=Duration.hours(1),
                )
            ],
        )

        CfnOutput(self, "RawDataBucketName", value=self.raw_data_bucket.bucket_name)
        CfnOutput(self, "HackerNewsIngestionLambdaName", value=self.hacker_news_ingestion.function_name)
        CfnOutput(self, "NormalizationLambdaName", value=self.normalization.function_name)
        CfnOutput(self, "GoldTransformationLambdaName", value=self.gold_transform.function_name)
        CfnOutput(self, "HackerNewsBronzePrefix", value="bronze/hackernews/")
        CfnOutput(self, "XBitcoinBronzePrefix", value="bronze/x/bitcoin/")
        CfnOutput(self, "SilverPrefix", value="silver/")
        CfnOutput(self, "GoldPrefix", value="gold/")
        CfnOutput(
            self,
            "DeploySteps",
            value=(
                "After deploy invoke GoldTransformation with {} or wait for 03:00 UTC cron"
            ),
        )
