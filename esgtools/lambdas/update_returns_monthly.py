import json
from ast import literal_eval

from esgtools.consolidation import merge
from esgtools.utils import aws


def lambda_handler(event, context):  # pylint: disable=unused-argument
    """Update returns_monthly table"""

    # Decrypts secret using the associated KMS key.
    db_credentials = literal_eval(aws.get_secret("prod/awsportfolio/key"))

    # Merge and update returns_monthly
    merge.merge_alpha_and_wrds_returns(sql_params=db_credentials)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "returns_monthly updated",
            }
        ),
    }
