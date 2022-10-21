import sys
import pandas as pd
from esgtools.etl import etl_alpha


def test():
    print("sys.path")
    assert 2==2
    assert 1==0, "1 should be equal to 0"


def test_get_last_business_date():
    pass

def test_get_api_prices_to_upload():
    print(sys.path)
    api_prices = pd.read_csv("esgtools/tests/data/api_prices_scenario_base.csv")
    db_prices = pd.read_csv("esgtools/tests/data/db_prices_scenario_1.csv")
    get_api_prices_to_upload( api_prices, db_prices, "compact")


    print(api_prices)
    print(db_prices)
    assert 1 == 0
