
import scraper


def main():
    print("Starting scraper!")
    tiingo_scraper = scraper.TiingoScraper()
    tiingo_scraper.process()
    print("Done!")


if __name__ == "__main__":
    main()
