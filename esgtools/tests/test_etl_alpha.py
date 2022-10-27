import sys
import os
import json
import pytest
import pandas as pd
from datetime import datetime
from pytz import timezone
from esgtools.etl import etl_alpha

tz = timezone('US/Pacific')

@pytest.mark.parametrize("datetime_run,date_exp", [
(datetime(2022, 10, 26, 18, 0, 0, tzinfo=tz), datetime(2022, 10, 26)),
(datetime(2022, 10, 26, 10, 0, 0, tzinfo=tz), datetime(2022, 10, 25)),
(datetime(2022, 10, 22, 22, 0, 0, tzinfo=tz), datetime(2022, 10, 21)),
(datetime(2023, 1, 2, 5, 0, 0, tzinfo=tz), datetime(2022, 12, 30)),
])
def test_get_last_business_date(datetime_run, date_exp):
    alpha = etl_alpha.AlphaScraper(connect=False, asof=datetime_run)
    date_act = alpha._get_last_business_date()
    assert date_exp.date() == date_act


@pytest.mark.parametrize("scenario", [1, 2, 3, 4, 5, 6])
def test_get_api_prices_to_upload(scenario):
    
    alpha = etl_alpha.AlphaScraper(connect=False)

    prices_path = "esgtools/tests/data/prices/"
    api_prices = pd.read_csv(f"{prices_path}/scenario_{scenario}/api_prices.csv")
    db_prices = pd.read_csv(f"{prices_path}/scenario_{scenario}/db_prices.csv")
    exp_api_prices = pd.read_csv(f"{prices_path}/scenario_{scenario}/output_df.csv")
    with open(f"{prices_path}/scenario_{scenario}/output_vars.json") as vars_json:
        output_vars = json.load(vars_json)

    size = output_vars["size"]
    actual_output = alpha._get_api_prices_to_upload(api_prices, db_prices, size)
    act_should_upload, act_clean_db_table, act_api_prices = actual_output
    exp_should_upload = output_vars["should_upload"]
    exp_clean_db_table = output_vars["clean_db_table"]
    assert act_should_upload == exp_should_upload
    assert act_clean_db_table == exp_clean_db_table
    assert type(act_api_prices) == type(exp_api_prices)
    assert exp_api_prices.equals(act_api_prices)

