import json
from ast import literal_eval

from esgtools.alpha import api, table
from esgtools.consolidation import merge
from esgtools.utils import aws


def lambda_handler(event, context):  # pylint: disable=unused-argument
    """Update assets table"""

    # Decrypts secret using the associated KMS key.
    db_credentials = literal_eval(aws.get_secret("prod/awsportfolio/key"))
    api_key = literal_eval(aws.get_secret("prod/AlphaApi/key"))["ALPHAVANTAGE_API_KEY"]

    alpha_scraper = api.AlphaScraper(api_key=api_key)
    alpha_assets = table.AlphaTableAssets(
        "assets_alpha",
        [],
        alpha_scraper,
        sql_params=db_credentials,
    )
    alpha_assets.update_all()

    merge.merge_alpha_and_wrds_assets(sql_params=db_credentials)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "assets updated",
            }
        ),
    }