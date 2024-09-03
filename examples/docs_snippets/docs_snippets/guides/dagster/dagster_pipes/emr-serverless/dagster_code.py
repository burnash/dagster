# start_asset_marker
import os

import boto3
from dagster_aws.pipes import PipesEMRServerlessClient

from dagster import AssetExecutionContext, asset


@asset
def emr_serverless_asset(
    context: AssetExecutionContext,
    pipes_emr_serverless_client: PipesEMRServerlessClient,
):
    return pipes_emr_serverless_client.run(
        context=context,
        start_job_run_params={
            "applicationId": "00fm4oe0607u5a1d",
            "clientToken": context.run_id,  # idempotency identifier for the job run
            "executionRoleArn": "arn:aws:iam::467123434025:role/service-role/AmazonEMR-ExecutionRole-1725370560012",
            "jobDriver": {
                "sparkSubmit": {
                    "entryPoint": "s3://aws-glue-assets-467123434025-eu-north-1/scripts/emr-serverless/wordcount.py",
                    "entryPointArguments": [
                        "s3://aws-glue-assets-467123434025-eu-north-1/envs/emr-serverless/outputs/word-count"
                    ],
                    "sparkSubmitParameters": "".join(
                        [
                            " --conf spark.executor.cores=1",
                            " --conf spark.executor.memory=4g",
                            " --conf spark.driver.cores=1",
                            " --conf spark.driver.memory=4g",
                            " --conf spark.executor.instances=1",
                        ]
                    ),
                }
            },
            "configurationOverrides": {
                "monitoringConfiguration": {
                    "cloudWatchLoggingConfiguration": {"enabled": True}
                }
            },
        },
    ).get_materialize_result()


# end_asset_marker

# start_definitions_marker

from dagster import Definitions  # noqa


defs = Definitions(
    assets=[emr_serverless_asset],
    resources={"pipes_emr_serverless_client": PipesEMRServerlessClient()},
)

# end_definitions_marker
