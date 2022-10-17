
from datetime import datetime
from etl import etl_alpha


def main():
    print("Starting scraper!")
    alpha = etl_alpha.AlphaScraper()

    size = 'full'
    alpha.refresh_all_prices(size)
    print("Done!")


if __name__ == "__main__":
    main()
