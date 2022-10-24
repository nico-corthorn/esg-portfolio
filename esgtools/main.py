
from datetime import datetime
from esgtools.etl import etl_alpha


def main():
    print("Starting scraper!")
    alpha = etl_alpha.AlphaScraper()

    size = 'compact'
    #alpha.refresh_all_prices(size)
    alpha.update_prices_symbol('AAPL', size)
    #alpha._get_db_prices("Catalina")
    print("Done!")


if __name__ == "__main__":
    main()
