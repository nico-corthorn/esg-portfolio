import os
import json
import boto3
from botocore.exceptions import ClientError
from ast import literal_eval

from utils import sql_manager, aws, utils
from alpha import api, table


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    -------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    print("event")
    print(event)
    print()

    # Example 
    # http://127.0.0.1:3000/update-prices?size=compact&symbols=AMZN,AAPL,MSFT
    # 'queryStringParameters': {'parallel': '1', 'size': 'compact', 'symbols': 'AMZN,AAPL,MSFT'}

    # Inputs
    if 'queryStringParameters' in event:
        inputs = event["queryStringParameters"]
    else:
        inputs = event

    # Gather parameters
    size = inputs["size"] if "size" in inputs else "compact"
    symbols = inputs["symbols"].split(",") if "symbols" in inputs else []
    parallel = utils.str2bool(inputs["parallel"]) if "parallel" in inputs else False
    print(f"size = {size}")
    print(f"symbols = {symbols}")
    print(f"parallel = {parallel}")

    # Decrypts secret using the associated KMS key.
    db_credentials = literal_eval(aws.get_secret("prod/awsportfolio/key"))
    api_key = literal_eval(aws.get_secret("prod/AlphaApi/key"))["ALPHAVANTAGE_API_KEY"]

    alpha_scraper = api.AlphaScraper(api_key=api_key)
    prices_keys = ["symbol", "date"]
    alpha_prices = table.AlphaTablePrices(
        "prices_alpha", 
        prices_keys, 
        alpha_scraper, 
        sql_params=db_credentials
    )
    alpha_prices_monthly = table.AlphaTablePricesMonthly(
        "prices_alpha_monthly",
        sql_params=db_credentials
    )

    if symbols:
        alpha_prices.update_list(symbols, size=size, parallel=parallel)
        alpha_prices_monthly.update_list(symbols, parallel=parallel)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Daily and monthly prices updated for symbols = {symbols}, size = {size}",
        }),
    }
