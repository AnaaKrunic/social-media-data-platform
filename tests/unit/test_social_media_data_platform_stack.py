import aws_cdk as core
import aws_cdk.assertions as assertions

from social_media_data_platform.social_media_data_platform_stack import SocialMediaDataPlatformStack


def test_stack_resources():
    app = core.App()
    stack = SocialMediaDataPlatformStack(app, "social-media-data-platform")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::S3::Bucket", 1)
    template.resource_count_is("AWS::Lambda::Function", 3)
    template.resource_count_is("AWS::EC2::Instance", 0)
    template.resource_count_is("AWS::EC2::VPC", 0)
    template.resource_count_is("AWS::Events::Rule", 1)
    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)
    template.resource_count_is("AWS::EC2::NatGateway", 0)
    template.resource_count_is("AWS::EC2::EIP", 0)
    template.resource_count_is("AWS::SecretsManager::Secret", 0)

    template.has_resource_properties("AWS::Events::Rule", {
        "ScheduleExpression": "cron(0 1 * * ? *)",
        "State": "DISABLED",
    })

    template.has_output("PipelineArn", {})
    template.has_output("GoldTransformationLambdaName", {})
    template.has_resource("AWS::S3::Bucket", {
        "DeletionPolicy": "Retain",
        "UpdateReplacePolicy": "Retain",
    })
    resources = template.to_json()["Resources"]
    for resource in resources.values():
        if resource["Type"] == "AWS::Lambda::Function":
            assert "VpcConfig" not in resource["Properties"]

    state_machine = next(
        resource for resource in resources.values()
        if resource["Type"] == "AWS::StepFunctions::StateMachine"
    )
    definition_parts = state_machine["Properties"]["DefinitionString"]["Fn::Join"][1]
    definition = "".join(part for part in definition_parts if isinstance(part, str))
    assert '\"date.$\":\"$\"' in definition
    assert "$$.Map.Item.Value" not in definition
