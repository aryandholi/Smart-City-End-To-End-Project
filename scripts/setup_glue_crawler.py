"""
scripts/setup_glue_crawler.py
─────────────────────────────────────────────────────────────────
Uses boto3 to:
  1. Ensure the IAM role for Glue exists (or creates a basic one)
  2. Create the Glue database 'smart_city_db'
  3. Create a Glue Crawler targeting the S3 bucket
  4. Start the crawler and poll until completion
  5. Print discovered table names

Prerequisites:
  • AWS credentials configured (env vars or ~/.aws/credentials)
  • S3_BUCKET_NAME set in environment / .env
  • IAM user must have GlueConsoleFullAccess + IAMReadOnlyAccess

Usage:
    python scripts/setup_glue_crawler.py
"""

import json
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv
load_dotenv()

from config.config import (
    S3_BUCKET_NAME,
    S3_OUTPUT_PREFIX,
    GLUE_DATABASE_NAME,
    GLUE_CRAWLER_NAME,
    GLUE_IAM_ROLE_NAME,
    AWS_DEFAULT_REGION,
)

GLUE_TRUST_POLICY = json.dumps({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
})

GLUE_INLINE_POLICY = json.dumps({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [
                f"arn:aws:s3:::{S3_BUCKET_NAME}",
                f"arn:aws:s3:::{S3_BUCKET_NAME}/*",
            ],
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
            ],
            "Resource": "*",
        },
    ],
})


# ── IAM helpers ────────────────────────────────────────────────

def ensure_glue_role(iam_client) -> str:
    """Return ARN of Glue IAM role, creating it if necessary."""
    try:
        role = iam_client.get_role(RoleName=GLUE_IAM_ROLE_NAME)
        arn = role["Role"]["Arn"]
        print(f"✅  IAM role already exists: {arn}")
        return arn
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise

    print(f"Creating IAM role '{GLUE_IAM_ROLE_NAME}'...")
    role = iam_client.create_role(
        RoleName=GLUE_IAM_ROLE_NAME,
        AssumeRolePolicyDocument=GLUE_TRUST_POLICY,
        Description="Glue service role for Smart City crawler",
    )
    arn = role["Role"]["Arn"]

    iam_client.put_role_policy(
        RoleName=GLUE_IAM_ROLE_NAME,
        PolicyName="SmartCityGlueS3Access",
        PolicyDocument=GLUE_INLINE_POLICY,
    )
    # Attach the AWS-managed Glue service policy too
    iam_client.attach_role_policy(
        RoleName=GLUE_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    )

    print(f"✅  IAM role created: {arn}  (waiting 15s for propagation...)")
    time.sleep(15)
    return arn


# ── Glue helpers ───────────────────────────────────────────────

def ensure_glue_database(glue_client):
    try:
        glue_client.get_database(Name=GLUE_DATABASE_NAME)
        print(f"✅  Glue database exists: {GLUE_DATABASE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        glue_client.create_database(
            DatabaseInput={
                "Name": GLUE_DATABASE_NAME,
                "Description": "Smart City streaming data catalog",
            }
        )
        print(f"✅  Glue database created: {GLUE_DATABASE_NAME}")


def ensure_glue_crawler(glue_client, role_arn: str):
    s3_target = f"s3://{S3_BUCKET_NAME}/{S3_OUTPUT_PREFIX}/"

    targets = {
        "S3Targets": [
            {"Path": f"{s3_target}vehicle_data/",   "Exclusions": []},
            {"Path": f"{s3_target}gps_data/",        "Exclusions": []},
            {"Path": f"{s3_target}weather_data/",    "Exclusions": []},
            {"Path": f"{s3_target}telemetry_data/",  "Exclusions": []},
            {"Path": f"{s3_target}emergency_data/",  "Exclusions": []},
        ]
    }

    try:
        glue_client.get_crawler(Name=GLUE_CRAWLER_NAME)
        print(f"✅  Crawler already exists: {GLUE_CRAWLER_NAME}")
        # Update targets in case they changed
        glue_client.update_crawler(
            Name=GLUE_CRAWLER_NAME,
            Role=role_arn,
            DatabaseName=GLUE_DATABASE_NAME,
            Targets=targets,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        glue_client.create_crawler(
            Name=GLUE_CRAWLER_NAME,
            Role=role_arn,
            DatabaseName=GLUE_DATABASE_NAME,
            Description="Crawls Smart City Parquet data from S3",
            Targets=targets,
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
            Configuration=json.dumps({
                "Version": 1.0,
                "CrawlerOutput": {"Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}},
            }),
        )
        print(f"✅  Crawler created: {GLUE_CRAWLER_NAME}")


def run_crawler_and_wait(glue_client):
    print(f"\n🚀  Starting crawler '{GLUE_CRAWLER_NAME}'...")
    glue_client.start_crawler(Name=GLUE_CRAWLER_NAME)

    elapsed = 0
    while True:
        time.sleep(15)
        elapsed += 15
        state = glue_client.get_crawler(Name=GLUE_CRAWLER_NAME)["Crawler"]["State"]
        print(f"   [{elapsed:>3}s] Crawler state: {state}")
        if state == "READY":
            break

    metrics = glue_client.get_crawler_metrics(CrawlerNameList=[GLUE_CRAWLER_NAME])
    last_run_metrics = metrics["CrawlerMetricsList"][0]
    print(f"\n✅  Crawler finished:")
    print(f"    Tables created:  {last_run_metrics.get('TablesCreated', 0)}")
    print(f"    Tables updated:  {last_run_metrics.get('TablesUpdated', 0)}")
    print(f"    Tables deleted:  {last_run_metrics.get('TablesDeleted', 0)}")


def list_tables(glue_client):
    tables = glue_client.get_tables(DatabaseName=GLUE_DATABASE_NAME)["TableList"]
    print(f"\n📋  Tables in '{GLUE_DATABASE_NAME}':")
    for t in tables:
        print(f"    • {t['Name']}  ({t.get('StorageDescriptor', {}).get('NumberOfBuckets', '?')} buckets)")


# ── Main ───────────────────────────────────────────────────────

def main():
    print("\n══════════════════════════════════════════════")
    print("   AWS Glue Crawler Setup — Smart City")
    print("══════════════════════════════════════════════")
    print(f"Region:  {AWS_DEFAULT_REGION}")
    print(f"Bucket:  s3://{S3_BUCKET_NAME}/{S3_OUTPUT_PREFIX}/")
    print(f"DB:      {GLUE_DATABASE_NAME}")
    print(f"Crawler: {GLUE_CRAWLER_NAME}\n")

    if not S3_BUCKET_NAME:
        print("❌  S3_BUCKET_NAME not set. Add it to your .env file.")
        sys.exit(1)

    session   = boto3.Session(region_name=AWS_DEFAULT_REGION)
    iam_client  = session.client("iam")
    glue_client = session.client("glue")

    role_arn = ensure_glue_role(iam_client)
    ensure_glue_database(glue_client)
    ensure_glue_crawler(glue_client, role_arn)
    run_crawler_and_wait(glue_client)
    list_tables(glue_client)

    print("\n🎉  Glue setup complete! You can now query the data in Amazon Athena.")


if __name__ == "__main__":
    main()
