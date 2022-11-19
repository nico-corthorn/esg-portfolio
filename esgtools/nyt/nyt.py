
# External
import os
import sys
import requests
import json
import pandas as pd
from datetime import datetime
import traceback

# Internal
from utils import sql_manager


class ManagerNYTdata:

    def __init__(self, clean_tables=False, api_key=None):
        # Initial parameters
        self.sql_manager = sql_manager.ManagerSQL()
        if api_key is None:
            self.api_key = self.get_api_key()

        # Clean
        if clean_tables:
            self.sql_manager.clean_table('nyt_archive')

    def get_api_key(self):
        with open('etl/keys.json', ) as f:
            keys = json.load(f)
            api_key = keys["nyt"]["Key"]
        return api_key

    def nyt_upload_all_articles(self, year_start=2001, verbose=0):
        now = datetime.now()
        for year in range(year_start, now.year + 1):
            month_end = 12
            if year == now.year:
                month_end = now.month - 1
            for month in range(1, month_end + 1):
                self.nyt_upload_one_month_articles(year, month, verbose)


    def nyt_upload_one_month_articles(self, year, month, verbose=0):
        try:
            bool_verbose = verbose > 0

            # Verify data has not been downloaded
            year_month = f"{year}{month:02}"
            db_year_month_list = self.sql_manager.select_distinct_column_list('year_month', 'nyt_archive')
            if year_month in db_year_month_list:
                if bool_verbose:
                    print(f"Database already has NYT articles for {year}-{month}")
                return

            # Download
            if bool_verbose:
                print(f"Downloading NYT {year_month}")
            t0 = datetime.now()
            df = self.nyt_api_download_one_month_articles(year, month)
            t1 = datetime.now()
            if bool_verbose:
                if df is None:
                    print(f"No data returned for {year_month} ({(t1 - t0).total_seconds():.2f} sec)")
                    return
                else:
                    print(f"Downloaded NYT {year_month} ({(t1 - t0).total_seconds():.2f} sec)")

            # Upload
            if bool_verbose:
                print(f"Uploading NYT {year_month}")
            t0 = datetime.now()
            self.sql_manager.upload_df_chunks('nyt_archive', df)
            t1 = datetime.now()
            if verbose > 0:
                print(f"Uploaded NYT {year_month} ({(t1 - t0).total_seconds():.2f} sec)")

        except Exception as e:
            t1 = datetime.now()
            print(f"ERROR: Failed to upload NYT {year_month} ({(t1 - t0).total_seconds():.2f} sec)")
            exc_type, exc_value, exc_tb = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_tb)

    def nyt_api_download_one_month_articles(self, year, month, lud=None):

        # Scrape nyt api
        url = f"https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={self.api_key}"
        if lud is None:
            lud = datetime.now()
        json_data = requests.get(url)
        data = json.loads(json_data.text)
        biz_articles = []
        for i, article in enumerate(data['response']['docs']):
            news_desk = article['news_desk']
            if news_desk == 'Business':
                org = [keyword['value'] for keyword in article['keywords'] if keyword['name'] == 'organizations']
                if org:
                    sub = [keyword['value'] for keyword in article['keywords'] if keyword['name'] == 'subject']
                    org_str = ' | '.join(org)
                    sub_str = ' | '.join(sub)
                    web_url = article['web_url']
                    pub_date = article['pub_date']
                    headline = article['headline']['main']
                    snippet = article['snippet']
                    biz_article = {
                        'web_url': web_url,
                        'pub_date': pub_date,
                        'organizations': org_str,
                        'subjects': sub_str,
                        'headline': headline,
                        'snippet': snippet,
                    }
                    biz_articles.append(biz_article)

        # Convert to DataFrame and minimal cleaning
        cols = ['web_url', 'pub_date', 'organizations', 'subjects', 'headline', 'snippet']
        df = pd.DataFrame(biz_articles)
        if df.empty:
            return None
        df = df[cols]
        df['pub_date'] = pd.to_datetime(df.pub_date)
        text_insert_cols = ['organizations', 'subjects', 'headline', 'snippet']
        df[text_insert_cols] = df[text_insert_cols].mask(df[text_insert_cols].isnull(), '')
        df['year_month'] = f"{year}{month:02}"
        df['lud'] = lud
        final_cols = ['web_url', 'year_month', 'pub_date', 'organizations', 'subjects', 'headline', 'snippet', 'lud']
        df = df[final_cols]

        return df


