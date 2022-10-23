import sys
import pandas as pd
from esgtools.etl import etl_alpha


def test_get_last_business_date():
    assert 1==1

def test_get_api_prices_to_upload():

    # Read api_prices
    # Scenarios 1, 2, 3, 5
    api_prices_base = pd.read_csv("esgtools/tests/data/api_prices_scenario_base.csv")
    # Scenario 4
    api_prices_4 = pd.read_csv("esgtools/tests/data/api_prices_scenario_4.csv")

    # Read db_prices
    db_prices_1 = pd.read_csv("esgtools/tests/data/db_prices_scenario_1.csv")
    db_prices_2 = pd.read_csv("esgtools/tests/data/db_prices_scenario_2.csv")
    db_prices_3 = pd.read_csv("esgtools/tests/data/db_prices_scenario_3.csv")
    db_prices_4 = pd.read_csv("esgtools/tests/data/db_prices_scenario_4.csv")
    db_prices_5 = pd.read_csv("esgtools/tests/data/db_prices_scenario_5.csv")

    # Generate expected outputs
    api_output_1 = None
    api_output_2 = api_prices_base.loc[api_prices_base.date == '10/18/22']

    alpha = etl_alpha.AlphaScraper()

    # Scenario 1
    # _get_api_prices_to_upload returns should_upload, clean_db_table, api_prices
    actual_output = alpha._get_api_prices_to_upload(api_prices_base, db_prices_1, "compact")
    act_should_upload, act_clean_db_table, act_api_prices = actual_output
    exp_should_upload = False
    exp_clean_db_table = False
    exp_api_prices = api_output_1
    assert act_should_upload == exp_should_upload
    assert act_clean_db_table == exp_clean_db_table
    assert type(act_api_prices) == type(exp_api_prices)
    assert act_api_prices == exp_api_prices

    # Scenario 2
    actual_output = alpha._get_api_prices_to_upload(api_prices_base, db_prices_2, "compact")
    act_should_upload, act_clean_db_table, act_api_prices = actual_output
    exp_should_upload = True
    exp_clean_db_table = False
    exp_api_prices = api_output_2
    assert act_should_upload == exp_should_upload
    assert act_clean_db_table == exp_clean_db_table
    assert type(act_api_prices) == type(exp_api_prices)
    assert exp_api_prices.equals(act_api_prices)
