
from datetime import datetime
from etl import etl_alpha


def main():
    print("Starting scraper!")
    alpha = etl_alpha.AlphaScraper()
    #data = alpha.download_all_listings(datetime.now())
    #data_clean = alpha.clean_listings(data)
    #assets = alpha.get_assets_table(data_clean)
    #print(assets)
    size = 'compact'
    alpha.update_prices_symbol('AMZN', size)
    #prices_msft = alpha.get_adjusted_prices('MSFT', size)
    #prices_aapl = alpha.get_adjusted_prices('AAPL', size)
    #updated_amzn = alpha.update_prices(prices_amzn, size)
    #updated_msft = alpha.update_prices(prices_msft, size)
    #updated_aapl = alpha.update_prices(prices_aapl, size)
    print("Done!")


if __name__ == "__main__":
    main()
