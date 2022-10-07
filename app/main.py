
from datetime import datetime
from etl import etl_alpha


def main():
    print("Starting scraper!")
    alpha = etl_alpha.AlphaScraper()
    #data = alpha.download_all_listings(datetime.now())
    #data_clean = alpha.clean_listings(data)
    #assets = alpha.get_assets_table(data_clean)
    #print(assets) 
    prices = alpha.get_adjusted_prices('ARVR')
    print(prices)
    print("Done!")


if __name__ == "__main__":
    main()
