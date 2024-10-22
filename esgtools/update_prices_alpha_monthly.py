import os
import json
import boto3
from botocore.exceptions import ClientError
from ast import literal_eval

from utils import sql_manager, aws, utils
from alpha import api, table


def lambda_handler(event, context):
    print("event", event)

    # Example 
    # http://127.0.0.1:3000/update-prices?size=compact&symbols=AMZN,AAPL,MSFT
    # 'queryStringParameters': {'parallel': '1', 'symbols': 'AMZN,AAPL,MSFT'}

    # Inputs
    if 'queryStringParameters' in event:
        inputs = event["queryStringParameters"]
    else:
        inputs = event

    # Gather parameters
    symbols = inputs["symbols"].split(",") if "symbols" in inputs else []
    parallel = utils.str2bool(inputs["parallel"]) if "parallel" in inputs else False
    print(f"symbols = {symbols}")
    print(f"parallel = {parallel}")

    # Decrypts secret using the associated KMS key.
    db_credentials = literal_eval(aws.get_secret("prod/awsportfolio/key"))

    alpha_prices_monthly = table.AlphaTablePricesMonthly(
        "prices_alpha_monthly",
        sql_params=db_credentials
    )

    if symbols:
        alpha_prices_monthly.update_list(symbols, parallel=parallel)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Monthly prices updated for symbols provided",
        }),
    }
