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
    # http://127.0.0.1:3000/get-assets?ref_table=prices_alpha&group=100
    # 'queryStringParameters': {'ref_table': 'prices_alpha', 'group': '100'}
    # {"queryStringParameters": {"ref_table": "prices_alpha", "group": "100"}}

    print("event['queryStringParameters']")
    print(event["queryStringParameters"])
    print()

    # Inputs
    inputs = event["queryStringParameters"]
    assert "ref_table" in inputs
    ref_table = inputs["ref_table"]
    validate = utils.str2bool(inputs["validate"]) \
                    if "validate" in inputs else False
    asset_types = inputs["asset_types"].split(",") \
                    if "asset_types" in inputs else ["Stock"]
    group = int(inputs["group"]) if "group" in inputs else 10
    print(f"ref_table = {ref_table}")
    print(f"validate = {validate}")
    print(f"asset_types = {asset_types}")
    print(f"group = {group}")

    # Decrypts secret using the associated KMS key.
    db_credentials = literal_eval(aws.get_secret("prod/awsportfolio/key"))
    api_key = literal_eval(aws.get_secret("prod/AlphaApi/key"))["ALPHAVANTAGE_API_KEY"]

    alpha_scraper = api.AlphaScraper(api_key=api_key)

    assets_sublists = []

    if ref_table == "prices_alpha":
        keys = ["symbol", "date"]
        alpha_prices = table.AlphaTablePrices(
            ref_table, 
            keys,
            alpha_scraper, 
            sql_params=db_credentials
        )
        assets = alpha_prices.get_assets(validate, asset_types)
        assets_sublists = [{"symbols": ','.join(list(assets.loc[i:i+group].symbol))} \
                                for i in range(0, len(assets), group)]
    else:
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
        if ref_table == "balance_alpha":
            assets = alpha_balance.get_assets(validate, asset_types)
            assets_sublists = [{"symbols": ','.join(list(assets.loc[i:i+group].symbol))} \
                                    for i in range(0, len(assets), group)]
        elif ref_table == "income_alpha":
            assets = alpha_income.get_assets(validate, asset_types)
            assets_sublists = [{"symbols": ','.join(list(assets.loc[i:i+group].symbol))} \
                                    for i in range(0, len(assets), group)]
        elif ref_table == "accounting":
            assets_balance = alpha_balance.get_assets(validate, asset_types)
            assets_income = alpha_income.get_assets(validate, asset_types)
            assets = list(set(assets_balance.symbol).union(set(assets_income.symbol)))
            assets.sort()
            assets_sublists = [{"symbols": ','.join(list(assets[i:i+group]))} \
                                    for i in range(0, len(assets), group)]
    
    print(assets_sublists)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"returning {len(assets_sublists)} sublists with maximum {group} symbols each",
        }),
        "assets": assets_sublists,
    }
