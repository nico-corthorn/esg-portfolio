import os
import json
import boto3
from botocore.exceptions import ClientError
from ast import literal_eval

from nyt.nyt_link import NytNewsLinker
from utils import sql_manager, aws, utils
from alpha import api, table


def lambda_handler(event, context):
    """ Update NYT articles. """
    print("event", event)

    # Example
    # {'queryStringParameters': {'org_to_asset_table': 'nyt_org_to_asset', 'news_to_asset_table': 'nyt_news_to_asset'}}

    # Inputs
    if 'queryStringParameters' in event:
        inputs = event["queryStringParameters"]
    else:
        inputs = event

    # Gather parameters
    org_to_asset_table = inputs.get("org_to_asset_table", "nyt_org_to_asset")
    news_to_asset_table = inputs.get("news_to_asset_table", "nyt_news_to_asset")
    print(f"org_to_asset_table = {org_to_asset_table}")
    print(f"news_to_asset_table = {news_to_asset_table}")

    # Decrypts secret using the associated KMS key.
    db_credentials = literal_eval(aws.get_secret("prod/awsportfolio/key"))

    # Update NYT articles
    nyt_linker = NytNewsLinker(org_to_asset_table, news_to_asset_table, db_credentials)
    nyt_linker.update_news_links()

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "New York Times article to asset links updated.",
        }),
    }
