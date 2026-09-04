# Social Media Data Platform

AWS CDK/Python Medallion pipeline for Hacker News and an offline X/Bitcoin
Tweets dataset.

## Data guarantees

- **Bronze** preserves official Hacker News HTTP response bytes and the original
  X CSV. Run metadata is stored separately under `control/bronze/`.
- **Silver** publishes immutable Parquet snapshots for `users`, `posts` and
  `post_relations`. A `control/silver/` pointer is written only after every
  table succeeds, so Gold cannot read a partial run.
- **Gold** reads only committed Silver snapshots and writes all required daily
  counts, user counts, top-10 rankings and Data Quality Score.
- Re-running a date creates a new snapshot and retains other dates/platforms.

HN karma and registration time come from the official user endpoint. Ask HN
records are official `story` items whose titles begin with `Ask HN`. Deleted HN
items return null, so the API gives no timestamp with which to assign them to a
day. Boundary discovery includes 1,000 overlapping IDs and every stored item is
then filtered by its exact UTC timestamp.

The X source is the original `Bitcoin_tweets_dataset_2.csv`. Its 5,484
structurally malformed CSV records are counted in the Silver manifest. The raw
file remains unchanged in Bronze.

## Local verification

From this directory, with the virtual environment active:

```powershell
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m pytest -q
$env:TEST_REAL_DATA = "1"
python -m pytest tests/test_pipeline_behaviour.py -q -s
Remove-Item Env:TEST_REAL_DATA
cdk synth
```

The opt-in test processes the full 74 MB source through in-memory S3, real
Parquet serialization, Silver deduplication and Gold metrics.

## Deployment

The deployment is intentionally serverless and contains only the first three
project layers. It creates no EC2 instance, NAT Gateway, Elastic IP, VPC or
Secrets Manager secret. The daily schedule is also disabled by default, so a
deploy does not start processing data on its own. S3, Lambda, Step Functions,
EventBridge and CloudWatch are usage-based AWS services; check your account's
Free Tier/credit limits and set an AWS Budget before running large jobs.

```powershell
aws sts get-caller-identity
aws configure get region
cdk bootstrap
cdk diff
cdk deploy --parameters AwsSdkPandasLayerVersion=31
```

Layer version 31 is current for Python 3.12 in `eu-central-1`. Check the AWS SDK
for pandas managed-layer table and override the parameter for another region.

Upload the original X source after deployment:

```powershell
python scripts/upload_bitcoin_bronze.py --bucket <RawDataBucketName> --dry-run
python scripts/upload_bitcoin_bronze.py --bucket <RawDataBucketName>
```

Run X once; the workflow normalizes it and creates Gold data for all nine dates:

```powershell
aws stepfunctions start-execution --state-machine-arn "<PipelineArn>" --input file://events/x_bitcoin.json
```

Run HN for a completed UTC date:

```powershell
aws stepfunctions start-execution --state-machine-arn "<PipelineArn>" --input file://events/hackernews.json
```

Inspect the execution in Step Functions and the corresponding
`control/bronze`, `control/silver` and `control/gold` objects in S3. Then enable
the daily schedule:

```powershell
cdk deploy -c enableSchedule=true --parameters AwsSdkPandasLayerVersion=31
```

## Verified AWS run

The deployed pipeline was verified in `eu-central-1` with the original Bitcoin
CSV and one complete Hacker News UTC day. X produced 36,238 users and 168,953
deduplicated posts from 174,438 input rows, while recording 5,484 malformed CSV
rows. Gold was committed for all nine dataset dates. The Hacker News run for
2026-09-03 committed 14,516 posts and 6,760 users, followed by all required
daily counts, user metrics, rankings and data-quality outputs.

## Deliberately not deployed yet

Visualization, Discord notifications and the VPC portion are kept out of this
safe deployment while Bronze, Silver and Gold are completed and verified. Their
source files may remain in the repository, but CDK does not provision or run
them. Add those optional project sections only after choosing an explicit cost
budget and an architecture suitable for the account's current AWS offer.
