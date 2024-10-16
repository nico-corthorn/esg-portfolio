import os
import json
import boto3
from botocore.exceptions import ClientError
from ast import literal_eval
import numpy as np
import pandas as pd

from utils import sql_manager, aws, utils
from alpha import api, table


def lambda_handler(event, context):
    """Get asset symbols that need to be refreshed

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format
        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

        Keys:
            ref_table: str
                Example: prices_alpha
            validate: str, will be converted to bool
                Example: True
            asset_types: str, will be converted to List[str] by splitting on ,
                Example: Stock or Stock,ETF
            max_assets_in_batch: str, will be converted to int
                Example: 4500

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    -------
    Dict using HTML protocol
    """
    print("event", event)

    # Example 
    # http://127.0.0.1:3000/get-assets?ref_table=prices_alpha&group=100
    # 'queryStringParameters': {'ref_table': 'prices_alpha'}
    # {"queryStringParameters": {"ref_table": "prices_alpha"}}

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
    #group = int(inputs["group"]) if "group" in inputs else 10
    max_assets_in_batch = int(inputs["max_assets_in_batch"]) if "max_assets_in_batch" in inputs else 75*60
    n_lists_in_group = int(inputs["n_lists_in_group"]) if "n_lists_in_group" in inputs else 10
    print(f"ref_table = {ref_table}")
    print(f"validate = {validate}")
    print(f"asset_types = {asset_types}")
    print(f"max_assets_in_batch = {max_assets_in_batch}")
    print(f"n_lists_in_group = {n_lists_in_group}")

    # Decrypts secret using the associated KMS key.
    db_credentials = literal_eval(aws.get_secret("prod/awsportfolio/key"))
    api_key = literal_eval(aws.get_secret("prod/AlphaApi/key"))["ALPHAVANTAGE_API_KEY"]

    alpha_scraper = api.AlphaScraper(api_key=api_key)

    assets = pd.DataFrame()
    if ref_table == "prices_alpha":
        keys = ["symbol", "date"]
        alpha_prices = table.AlphaTablePrices(
            ref_table, 
            keys,
            alpha_scraper, 
            sql_params=db_credentials
        )
        assets = alpha_prices.get_assets(validate, asset_types)
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
        elif ref_table == "income_alpha":
            assets = alpha_income.get_assets(validate, asset_types)
        elif ref_table == "accounting":
            assets_balance = alpha_balance.get_assets(validate, asset_types)
            assets_income = alpha_income.get_assets(validate, asset_types)
            assets = list(set(assets_balance.symbol).union(set(assets_income.symbol)))
            assets.sort()

    assets_sublists = []
    if assets.shape[0] > 0:
        for i in range(0, len(assets), max_assets_in_group):
            symbols_group: pd.Series = assets.loc[i:i+max_assets_in_group-1].symbol
            partition_group = np.array_split(symbols_group, n_lists_in_group)
            assets_sublists.append([{
                "symbols": ",".join(list(sublist)),
                "size": size
            } for sublist in partition_group])

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Returning {len(assets_sublists)} groups with {n_lists_in_group} sublists each.",
        }),
        "assets": assets_sublists,
    }