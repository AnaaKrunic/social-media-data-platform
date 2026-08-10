import aws_cdk as core
import aws_cdk.assertions as assertions

from social_media_data_platform.social_media_data_platform_stack import SocialMediaDataPlatformStack

# example tests. To run these tests, uncomment this file along with the example
# resource in social_media_data_platform/social_media_data_platform_stack.py
def test_bronze_ingestion_resources_created():
    app = core.App()
    stack = SocialMediaDataPlatformStack(app, "social-media-data-platform")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::S3::Bucket", 1)
    template.has_resource_properties("AWS::Lambda::Function", {
        "Handler": "handler.handler",
        "Runtime": "python3.12",
        "Timeout": 600,
        "MemorySize": 512,
    })
    template.has_resource_properties("AWS::Events::Rule", {
        "ScheduleExpression": "cron(0 1 * * ? *)",
        "State": "ENABLED",
    })
    template.resource_count_is("AWS::Lambda::Permission", 1)
