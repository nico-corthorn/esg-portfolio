
import pytest
import pandas as pd
from esgtools.etl import etl_alpha


@pytest.fixture
def prices():
    """
    """

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

    # Generate outputs
    api_output_1 = None
    api_output_2 = api_prices_base.loc[api_prices_base.date == '10/18/22']

    dir_map = {
        1: {
            "api_prices": api_prices_base,
            "db_prices_path": "esgtools/tests/data/db_prices_scenario_1.csv",
            "expected_output": (False, False, None),
        },
        2: {
            "api_prices": api_prices_base,
            "db_prices_path": "esgtools/tests/data/db_prices_scenario_2.csv",
            "expected_output": (False, False, None),
        }
    }
    for scenario, scenario_dct in dir_map.items():
        for key, val in scenario_dct.items():
            if key.endswith("_path"):
                data_name = key.replace("_path", "")
                #dir_map[scenario][data_name] = pd.read_csv(val)

    yield dir_map

