import aws_cdk as core
import aws_cdk.assertions as assertions

from social_media_data_platform.social_media_data_platform_stack import SocialMediaDataPlatformStack


def test_stack_resources():
    app = core.App()
    stack = SocialMediaDataPlatformStack(app, "social-media-data-platform")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::S3::Bucket", 1)
    template.resource_count_is("AWS::Lambda::Function", 3)
    template.resource_count_is("AWS::Events::Rule", 3)

    template.has_resource_properties("AWS::Lambda::Function", {
        "Handler": "handler.handler",
        "Runtime": "python3.12",
        "Timeout": 300,
        "MemorySize": 1024,
    })

    template.has_resource_properties("AWS::Events::Rule", {
        "ScheduleExpression": "cron(0 3 * * ? *)",
        "State": "ENABLED",
    })

    template.has_output("GoldPrefix", {"Value": "gold/"})
