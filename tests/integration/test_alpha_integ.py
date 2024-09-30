
from esgtools.alpha import table, api


def test_prices_alpha_monthly_update():

    alpha_prices_monthly = table.AlphaTablePricesMonthly("prices_alpha_monthly")
    alpha_prices_monthly.update("AAPL")
    assert True

