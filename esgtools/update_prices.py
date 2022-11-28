import os
import json
import boto3
from botocore.exceptions import ClientError
from ast import literal_eval

from utils import sql_manager
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
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    db_secret_name = "prod/awsportfolio/key"
    api_secret_name = "prod/AlphaApi/key"	
    region_name = "us-east-2"

    print(f"os.cpu_count() = {os.cpu_count()}")

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        db_secret_response = client.get_secret_value(SecretId=db_secret_name)
        api_secret_response = client.get_secret_value(SecretId=api_secret_name)
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    db_credentials = literal_eval(db_secret_response['SecretString'])
    api_key = literal_eval(api_secret_response['SecretString'])

    alpha_scraper = api.AlphaScraper(api_key=api_key)
    alpha_assets = table.AlphaTableAssets(
            "assets_alpha", [], alpha_scraper, sql_params=db_credentials, max_workers=os.cpu_count())
    alpha_assets.update_all()

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "assets updated",
        }),
    }
