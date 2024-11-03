import os
import json
import boto3
from botocore.exceptions import ClientError
from ast import literal_eval

from nyt.nyt_scrape import NytNewsScraper
from utils import sql_manager, aws, utils
from alpha import api, table


def lambda_handler(event, context):
    """ Update NYT articles. """
    print("event", event)

    # Example
    # {'queryStringParameters': {'table_name': 'nyt_archive', 'year_start': 2001, 'clean_table': 'False'}}

    # Inputs
    if 'queryStringParameters' in event:
        inputs = event["queryStringParameters"]
    else:
        inputs = event

    # Gather parameters
    table_name = inputs.get("table_name", "nyt_archive")
    year_start = int(inputs.get("year_start", 2001))
    clean_table = utils.str2bool(inputs.get("clean_table", "False"))
    verbose = utils.str2bool(inputs.get("verbose", "False"))
    print(f"table_name = {table_name}")
    print(f"year_start = {year_start}")
    print(f"clean_table = {clean_table}")
    print(f"verbose = {verbose}")

    # Decrypts secret using the associated KMS key.
    db_credentials = literal_eval(aws.get_secret("prod/awsportfolio/key"))
    api_key = literal_eval(aws.get_secret("prod/NYTApi/key"))["NYT_API_KEY"]

    # Update NYT articles
    nyt_scraper = NytNewsScraper(table_name, api_key, db_credentials)
    nyt_scraper.nyt_upload_all_articles(year_start, clean_table, verbose)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "New York Times articles updated.",
        }),
    }
