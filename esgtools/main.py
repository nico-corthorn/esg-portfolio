
from datetime import datetime
from esgtools.etl import etl_alpha_api, etl_alpha_table


def main():
    print("Starting scraper!")
    alpha_scraper1 = etl_alpha_api.AlphaScraper()
    alpha_scraper2 = etl_alpha_api.AlphaScraper()
    alpha_assets = etl_alpha_table.AlphaTableAssets("assets_alpha", [], [], alpha_scraper1)
    alpha_prices = etl_alpha_table.AlphaTablePrices("prices_alpha", [], [], alpha_scraper2)
    #alpha_balance = etl_alpha_table.AlphaTableBalance()

    size = 'compact'
    alpha_prices.update_all(size=size)


    #alpha.refresh_all_prices(size)
    #alpha.update_prices_symbol('AAPL', size)
    #alpha.update_balance_symbol('AMZN')
    #alpha.refresh_balances()
    print("Done!")


if __name__ == "__main__":
    main()
