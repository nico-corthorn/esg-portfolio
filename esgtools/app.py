import os
import json
import boto3
from botocore.exceptions import ClientError
from utils import sql_manager
from ast import literal_eval


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
        get_secret_value_response = client.get_secret_value(
            SecretId=db_secret_name
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = literal_eval(get_secret_value_response['SecretString'])

    sql = sql_manager.ManagerSQL(secret)
    query = "SELECT * FROM assets_alpha LIMIT 1"
    print(sql.select_query(query))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
            # "location": ip.text.replace("\n", "")
        }),
    }
