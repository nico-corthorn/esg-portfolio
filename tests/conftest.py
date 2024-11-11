# tests/conftest.py

from ast import literal_eval
from datetime import datetime

import pytest

from esgtools.domain_models.io import SQLParams, convert_dict_to_sql_params
from esgtools.utils import aws, date_utils, sql_manager


@pytest.fixture(scope="session")
def api_key():
    """Fixture to get API key from AWS Secrets"""
    try:
        return literal_eval(aws.get_secret("prod/AlphaApi/key"))["ALPHAVANTAGE_API_KEY"]
    except Exception as e:
        print(f"Failed to get API key: {e}. Returning demo")
        return "demo"


@pytest.fixture(scope="session")
def sql_params() -> SQLParams:
    """Fixture to get database credentials from AWS Secrets"""
    db_credentials = literal_eval(aws.get_secret("prod/awsportfolio/key"))
    return convert_dict_to_sql_params(db_credentials)


@pytest.fixture(scope="session")
def sql(sql_params: SQLParams):
    """Fixture to create SQL manager"""
    return sql_manager.ManagerSQL(sql_params)


@pytest.fixture
def last_business_date():
    """Fixture to get last business date"""
    return date_utils.get_last_business_date(asof=datetime.now())
