import os
import json
import boto3
from botocore.exceptions import ClientError
from ast import literal_eval

from utils import sql_manager, aws, utils
from alpha import api, table


def lambda_handler(event, context):
    """ Update prices_alpha for symbols given in event. """
    print("event", event)

    # Example 
    # http://127.0.0.1:3000/update-prices?size=compact&symbols=AMZN,AAPL,MSFT
    # 'queryStringParameters': {'parallel': '1', 'size': 'compact', 'symbols': 'AMZN,AAPL,MSFT'}

    # Inputs
    if 'queryStringParameters' in event:
        inputs = event["queryStringParameters"]
    else:
        inputs = event

    # Gather parameters
    size = inputs.get("size", "full")
    symbols = inputs["symbols"].split(",") if "symbols" in inputs else []
    parallel = utils.str2bool(inputs.get("parallel", "false"))
    print(f"size = {size}")
    print(f"symbols = {symbols}")
    print(f"parallel = {parallel}")

    # Decrypts secret using the associated KMS key.
    db_credentials = literal_eval(aws.get_secret("prod/awsportfolio/key"))
    api_key = literal_eval(aws.get_secret("prod/AlphaApi/key"))["ALPHAVANTAGE_API_KEY"]

    alpha_scraper = api.AlphaScraper(api_key=api_key, wait=False)
    prices_keys = ["symbol", "date"]
    alpha_prices = table.AlphaTablePrices(
        "prices_alpha", 
        prices_keys, 
        alpha_scraper, 
        sql_params=db_credentials
    )

    if symbols:
        alpha_prices.update_list(symbols, size=size, parallel=parallel)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Daily prices updated for symbols = {symbols}, size = {size}",
        }),
        "symbols": ",".join(symbols),
        "size": size
    }
