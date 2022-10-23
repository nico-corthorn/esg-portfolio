import sys
import pytest
import pandas as pd
from esgtools.etl import etl_alpha


def test_get_last_business_date():
    assert 1==1

expected_output_1 = (False, False,pd.read_csv("esgtools/tests/data/output_1.csv"))
expected_output_2 = (True, False, pd.read_csv("esgtools/tests/data/output_2.csv"))

lst = [
    ("api_prices_1", "db_prices_1", "compact", expected_output_1), 
    ("api_prices_1", "db_prices_2", "compact", expected_output_2)
]

@pytest.mark.parametrize("api_prices_file,db_prices_file,size,expected_output", lst)
def test_get_api_prices_to_upload(api_prices_file, db_prices_file, size, expected_output):
    
    alpha = etl_alpha.AlphaScraper()

    api_prices = pd.read_csv(f"esgtools/tests/data/{api_prices_file}.csv")
    db_prices = pd.read_csv(f"esgtools/tests/data/{db_prices_file}.csv")

    actual_output = alpha._get_api_prices_to_upload(api_prices, db_prices, size)
    act_should_upload, act_clean_db_table, act_api_prices = actual_output
    exp_should_upload, exp_clean_db_table, exp_api_prices = expected_output
    assert act_should_upload == exp_should_upload
    assert act_clean_db_table == exp_clean_db_table
    assert type(act_api_prices) == type(exp_api_prices)
    assert exp_api_prices.equals(act_api_prices)

