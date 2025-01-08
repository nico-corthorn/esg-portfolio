import json

import pandas as pd
import pytest

from tba_invest_etl.alpha import api, table
from tba_invest_etl.domain_models.io import SQLParams


@pytest.mark.parametrize("scenario", [1, 2, 3, 4, 5, 6])
def test_get_api_prices_to_upload(scenario: int, sql_params: SQLParams):
    alpha_scraper = api.AlphaScraper()
    prices_keys = ["symbol", "date"]
    alpha_prices = table.AlphaTablePrices("prices_alpha", prices_keys, alpha_scraper, sql_params)

    prices_path = "tests/unit/data/prices"
    api_prices = pd.read_csv(f"{prices_path}/scenario_{scenario}/api_prices.csv")
    db_prices = pd.read_csv(f"{prices_path}/scenario_{scenario}/db_prices.csv")
    exp_api_prices = pd.read_csv(f"{prices_path}/scenario_{scenario}/output_df.csv")
    with open(f"{prices_path}/scenario_{scenario}/output_vars.json", encoding="utf-8") as vars_json:
        output_vars = json.load(vars_json)

    size = output_vars["size"]
    actual_output = alpha_prices.get_api_prices_to_upload(api_prices, db_prices, size)
    act_should_upload, act_clean_db_table, act_api_prices = actual_output
    exp_should_upload = output_vars["should_upload"]
    exp_clean_db_table = output_vars["clean_db_table"]
    assert act_should_upload == exp_should_upload
    assert act_clean_db_table == exp_clean_db_table
    assert isinstance(act_api_prices, type(exp_api_prices))
    assert exp_api_prices.equals(act_api_prices)
