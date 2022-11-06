
from datetime import datetime
from esgtools.etl import etl_alpha_api, etl_alpha_table


def main():
    print("Starting scraper!")
    alpha_scraper = etl_alpha_api.AlphaScraper()
    prices_keys = ["symbol", "date"]  # change schema
    balance_keys = ["symbol", "report_type", "report_date", "currency", "account_name"]
    alpha_assets = etl_alpha_table.AlphaTableAssets("assets_alpha", [], alpha_scraper)
    alpha_prices = etl_alpha_table.AlphaTablePrices("prices_alpha", prices_keys, alpha_scraper)
    alpha_balance = etl_alpha_table.AlphaTableBalance("balance_alpha", balance_keys, alpha_scraper)

    size = 'compact'
    alpha_prices.update("AAPL", size=size)
    alpha_balance.update("AAPL")

    print("Done!")


if __name__ == "__main__":
    main()
