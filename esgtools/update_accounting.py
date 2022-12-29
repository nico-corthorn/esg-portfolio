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
    # http://127.0.0.1:3000/update-prices?symbols=AMZN,AAPL,MSFT
    # 'queryStringParameters': {'parallel': '0', 'symbols': 'AMZN,AAPL,MSFT'}

    # Inputs
    if 'queryStringParameters' in event:
        print("event['queryStringParameters']")
        print(event["queryStringParameters"])
        print()
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
    api_key = literal_eval(aws.get_secret("prod/AlphaApi/key"))["ALPHAVANTAGE_API_KEY"]

    alpha_scraper = api.AlphaScraper(api_key=api_key)
    accounting_keys = ["symbol", "report_type", "report_date", "currency", "account_name"]
    balance_accounts = ['totalAssets', 'commonStock', 'commonStockSharesOutstanding']
    alpha_balance = table.AlphaTableAccounting(
        "balance_alpha", 
        "BALANCE_SHEET", 
        accounting_keys, 
        alpha_scraper, 
        balance_accounts,
        sql_params=db_credentials
    )
    income_accounts = ['netIncome']
    alpha_income = table.AlphaTableAccounting(
        "income_alpha", 
        "INCOME_STATEMENT", 
        accounting_keys, 
        alpha_scraper, 
        income_accounts,
        sql_params=db_credentials
    )

    if symbols:

        print("Update balance sheet")
        alpha_balance.update_list(symbols, parallel=parallel)

        print("Update income statement")
        alpha_income.update_list(symbols, parallel=parallel)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Accounting updated for symbols = {symbols}",
        }),
    }
