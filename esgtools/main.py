
from datetime import datetime
from esgtools.alpha import api, table
from esgtools.consolidation import merge

def main():
    print("Starting scraper!")
    alpha_scraper = api.AlphaScraper()

    # Assets
    #alpha_assets = etl_alpha_table.AlphaTableAssets("assets_alpha", [], alpha_scraper)

    # Prices
    #prices_keys = ["symbol", "date"]  # change schema
    #alpha_prices = etl_alpha_table.AlphaTablePrices("prices_alpha", prices_keys, alpha_scraper)
    #size = 'compact'
    #alpha_prices.update("AAPL", size=size)

    # Accounting tables
    #accounting_keys = ["symbol", "report_type", "report_date", "currency", "account_name"]

    # Balance
    #balance_accounts = ['totalAssets', 'commonStock', 'commonStockSharesOutstanding']
    #alpha_balance = etl_alpha_table.AlphaTableAccounting("balance_alpha", "BALANCE_SHEET", accounting_keys, alpha_scraper, balance_accounts, max_workers=6)
    #alpha_balance.update("AAPL")

    # Income
    #income_accounts = ['netIncome']
    #alpha_income = table.AlphaTableAccounting("income_alpha", "INCOME_STATEMENT", accounting_keys, alpha_scraper, income_accounts)
    #alpha_income.update("AAPL")
    #alpha_income.update_all()

    # Prices monthly
    #alpha_prices_monthly = table.AlphaTablePricesMonthly("prices_alpha_monthly")
    #alpha_prices_monthly.update("AAPL")

    # Consolidation
    #merge.update_prices_alpha_monthly()
    merge.merge_alpha_and_wrds_returns()

    print("Done!")


if __name__ == "__main__":
    main()
