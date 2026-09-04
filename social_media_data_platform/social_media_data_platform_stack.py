"""AWS infrastructure for the Bronze, Silver and Gold data pipeline.

The default deployment intentionally contains no always-on compute and no NAT
Gateway.  The EventBridge schedule is disabled unless explicitly enabled with
`-c enableSchedule=true`.
"""

from pathlib import Path

from aws_cdk import (
    CfnOutput,
    CfnParameter,
    Duration,
    RemovalPolicy,
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


ROOT = Path(__file__).resolve().parents[1]


class SocialMediaDataPlatformStack(Stack):
    """Serverless Medallion pipeline for Hacker News and the X dataset."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # A deployment never starts the pipeline unless the user opts in.
        schedule_enabled = self.node.try_get_context("enableSchedule") == "true"

        self.raw_data_bucket = s3.Bucket(
            self,
            "RawDataBucket",
            # Pipeline snapshots use unique object keys. Bucket versioning would
            # retain overwritten control files and unnecessarily grow storage.
            versioned=False,
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
        )

        pandas_layer_version = CfnParameter(
            self,
            "AwsSdkPandasLayerVersion",
            type="Number",
            default=31,
            description="AWS SDK for pandas Python 3.12 managed layer version",
        )
        pandas_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            "AwsSdkPandasLayer",
            (
                f"arn:aws:lambda:{self.region}:336392948345:"
                f"layer:AWSSDKPandas-Python312:"
                f"{pandas_layer_version.value_as_number}"
            ),
        )

        def pipeline_function(
            name: str,
            directory: str,
            *,
            memory: int = 512,
            use_pandas: bool = False,
        ) -> lambda_.Function:
            # Functions deliberately run outside a VPC. HN ingestion needs the
            # public API, while S3 access is protected by prefix-scoped IAM.
            return lambda_.Function(
                self,
                name,
                runtime=lambda_.Runtime.PYTHON_3_12,
                handler="handler.handler",
                code=lambda_.Code.from_asset(
                    str(ROOT / "lambda" / directory),
                    exclude=["__pycache__", "*.pyc"],
                ),
                timeout=Duration.minutes(15),
                memory_size=memory,
                layers=[pandas_layer] if use_pandas else [],
                environment={"DATA_BUCKET_NAME": self.raw_data_bucket.bucket_name},
            )

        self.hacker_news_ingestion = pipeline_function(
            "HackerNewsIngestion", "ingestion"
        )
        # The ingestion handler kept its original environment variable name.
        self.hacker_news_ingestion.add_environment(
            "RAW_BUCKET_NAME", self.raw_data_bucket.bucket_name
        )
        self.normalization = pipeline_function(
            "SilverNormalization", "normalization", memory=3008, use_pandas=True
        )
        self.gold_transform = pipeline_function(
            "GoldTransformation", "gold", memory=3008, use_pandas=True
        )

        # Grant each layer only the S3 prefixes that it consumes or produces.
        def grant_data_access(fn, *, read=(), write=()):
            bucket_arn = self.raw_data_bucket.bucket_arn
            if read:
                fn.add_to_role_policy(
                    iam.PolicyStatement(
                        actions=["s3:GetObject"],
                        resources=[
                            f"{bucket_arn}/{prefix}*" for prefix in read
                        ],
                    )
                )
                fn.add_to_role_policy(
                    iam.PolicyStatement(
                        actions=["s3:ListBucket"],
                        resources=[bucket_arn],
                        conditions={
                            "StringLike": {
                                "s3:prefix": [
                                    f"{prefix}*" for prefix in read
                                ]
                            }
                        },
                    )
                )
            if write:
                fn.add_to_role_policy(
                    iam.PolicyStatement(
                        actions=["s3:PutObject"],
                        resources=[
                            f"{bucket_arn}/{prefix}*" for prefix in write
                        ],
                    )
                )

        grant_data_access(
            self.hacker_news_ingestion,
            write=("bronze/hackernews/", "control/bronze/"),
        )
        grant_data_access(
            self.normalization,
            read=("bronze/", "control/bronze/"),
            write=("silver/", "control/silver/"),
        )
        grant_data_access(
            self.gold_transform,
            read=("silver/", "control/silver/"),
            write=("gold/", "control/gold/"),
        )

        # HN route: plan -> parallel raw batches -> atomic Bronze commit ->
        # Silver normalization -> Gold metrics.
        plan = tasks.LambdaInvoke(
            self,
            "PlanDailyIngestion",
            lambda_function=self.hacker_news_ingestion,
            payload_response_only=True,
            result_path="$.plan",
        )
        collect_batch = tasks.LambdaInvoke(
            self,
            "CollectBatch",
            lambda_function=self.hacker_news_ingestion,
            payload_response_only=True,
        )
        collect_batch.add_retry(
            errors=["States.TaskFailed"],
            interval=Duration.seconds(10),
            max_attempts=2,
        )
        collect_all = sfn.Map(
            self,
            "CollectAllItems",
            items_path="$.plan.batches",
            max_concurrency=4,
            result_path=sfn.JsonPath.DISCARD,
        )
        collect_all.item_processor(collect_batch)

        commit_bronze = tasks.LambdaInvoke(
            self,
            "CommitBronze",
            lambda_function=self.hacker_news_ingestion,
            payload=sfn.TaskInput.from_object(
                {
                    "operation": "commit",
                    "plan": sfn.JsonPath.object_at("$.plan"),
                }
            ),
            payload_response_only=True,
            result_path="$.job",
        )
        normalize_silver = tasks.LambdaInvoke(
            self,
            "NormalizeSilver",
            lambda_function=self.normalization,
            payload=sfn.TaskInput.from_json_path_at("$.job"),
            result_path=sfn.JsonPath.DISCARD,
        )
        compute_gold = tasks.LambdaInvoke(
            self,
            "ComputeGold",
            lambda_function=self.gold_transform,
            payload=sfn.TaskInput.from_json_path_at("$.job"),
            result_path=sfn.JsonPath.DISCARD,
        )
        complete_hn = sfn.Succeed(self, "CompleteHackerNews")
        normalize_silver.next(compute_gold).next(complete_hn)

        # X route: the CSV is uploaded to Bronze once, normalized once and Gold
        # is generated for every date found in the dataset.
        normalize_x = tasks.LambdaInvoke(
            self,
            "NormalizeXSilver",
            lambda_function=self.normalization,
            payload=sfn.TaskInput.from_object(
                {"source": "x_bitcoin", "key.$": "$.key"}
            ),
            payload_response_only=True,
            result_path="$.xresult",
        )
        compute_x_date = tasks.LambdaInvoke(
            self,
            "ComputeXGoldDate",
            lambda_function=self.gold_transform,
            payload=sfn.TaskInput.from_object(
                # The Map item is already the state's scalar input, e.g.
                # "2023-02-25". Refer to it directly with "$".
                {"date.$": "$"}
            ),
            payload_response_only=True,
        )
        compute_all_x_dates = sfn.Map(
            self,
            "ComputeAllXGoldDates",
            items_path="$.xresult.dates",
            max_concurrency=1,
            result_path=sfn.JsonPath.DISCARD,
        )
        compute_all_x_dates.item_processor(compute_x_date)
        complete_x = sfn.Succeed(self, "CompleteX")

        route = sfn.Choice(self, "Source")
        route.when(
            sfn.Condition.and_(
                sfn.Condition.is_present("$.source"),
                sfn.Condition.string_equals("$.source", "x_bitcoin"),
            ),
            normalize_x.next(compute_all_x_dates).next(complete_x),
        )
        route.otherwise(
            plan.next(collect_all)
            .next(commit_bronze)
            .next(normalize_silver)
        )

        self.pipeline = sfn.StateMachine(
            self,
            "DailyPipeline",
            definition_body=sfn.DefinitionBody.from_chainable(route),
            timeout=Duration.hours(4),
        )

        events.Rule(
            self,
            "DailyHackerNewsIngestion",
            enabled=schedule_enabled,
            schedule=events.Schedule.cron(minute="0", hour="1"),
            targets=[
                targets.SfnStateMachine(
                    self.pipeline,
                    input=events.RuleTargetInput.from_object({}),
                )
            ],
        )

        outputs = {
            "RawDataBucketName": self.raw_data_bucket.bucket_name,
            "PipelineArn": self.pipeline.state_machine_arn,
            "HackerNewsIngestionLambdaName": (
                self.hacker_news_ingestion.function_name
            ),
            "NormalizationLambdaName": self.normalization.function_name,
            "GoldTransformationLambdaName": self.gold_transform.function_name,
            "GoldPrefix": "gold/",
        }
        for name, value in outputs.items():
            CfnOutput(self, name, value=value)
