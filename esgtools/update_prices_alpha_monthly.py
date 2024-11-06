import json
from ast import literal_eval

from esgtools.alpha import table
from esgtools.utils import aws


def lambda_handler(event, context):  # pylint: disable=unused-argument
    print("event", event)

    # Example
    # {'symbols': 'AMZN,AAPL,MSFT'}

    # Gather parameters
    symbols = event["symbols"].split(",") if "symbols" in event else []
    print(f"symbols = {symbols}")

    # Decrypts secret using the associated KMS key.
    db_credentials = literal_eval(aws.get_secret("prod/awsportfolio/key"))

    alpha_prices_monthly = table.AlphaTablePricesMonthly(
        "prices_alpha_monthly", sql_params=db_credentials
    )

    if symbols:
        alpha_prices_monthly.update_list(symbols)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Monthly prices updated for symbols provided",
            }
        ),
    }
