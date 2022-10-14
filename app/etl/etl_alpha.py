
import os
import csv
import requests
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import BDay

from utils import sql_manager, utils

URL_BASE = 'https://www.alphavantage.co/query?function='


class AlphaScraper():

    def __init__(self):
        self.sql = sql_manager.ManagerSQL()
        self.today = datetime.now().date()
        self.last_business_date = (self.today - BDay(1)).date()
        self.min_date = pd.to_datetime('1960-01-01').date()
        self.api_key = os.environ.get('ALPHAVANTAGE_API_KEY')


    def download_active_listings(self):
        
        url = f'{URL_BASE}LISTING_STATUS&apikey=demo'
        
        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            data = pd.DataFrame(my_list[1:], columns=my_list[0])
        
        return data


    def download_delisted(self, date_input):
        """
            Date is datetime.datetime or datetime.date
        """
        
        dte = date_input.strftime("%Y-%m-%d")
        
        url = f'{URL_BASE}LISTING_STATUS&date={dte}&state=delisted&apikey={self.api_key}'

        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            data = pd.DataFrame(my_list[1:], columns=my_list[0])
        
        return data
    

    def download_all_listings(self, date_input):
        
        # Download active
        data_active = self.download_active_listings()
        
        # Download delisted
        data_delist = self.download_delisted(date_input)
        
        # Concatenate
        data = data_active.append(data_delist).reset_index(drop=True)

        # Rename columns
        data.columns = [utils.camel_to_snake(col) for col in data.columns]
        
        # Fix missing values in delisting_date column
        data.loc[data.delisting_date == 'null', 'delisting_date'] = None
        
        return data
    

    def clean_listings(self, data):
        """
        data is assumed to have Active listings first and then Delisted.
        
        Listings downloaded from Alpha Vantage can be duplicated when using symbol, name and asset_type
        as keys.
        
        Cases identified
            1. Change of exchange
            2. Delisted and then re-listed
            3. Another company took the symbol
            4. Ilogical
        
        If symbol, name and asset_type are the same, then the first IPO date 
        and last delisting (or none if applicable) will be kept. Exchange is ignored.
        If name is different it is assumed that a different company took the symbol.
        If asset_type is different then it is assumed linked to a different asset.
        """
        
        # Keys and columns
        col_keys = ['symbol', 'name', 'asset_type']
        final_cols = ['symbol', 'name', 'asset_type', 'ipo_date', 'delisting_date']
        
        # Symbol-name-asset_type rows with no duplicates
        data_no_dup = data.drop_duplicates(subset=col_keys, keep=False)
        
        # Rest has duplicates
        ind_dup = [ind for ind in data.index if ind not in data_no_dup.index]
        data_dup = data.iloc[ind_dup]

        # Since Active listings are first we use those for delisting_date
        data_dup_first = data_dup.drop_duplicates(subset=col_keys, keep='first')

        # From all other rows we compute the first ipo_date
        data_dup_ipoDate = data_dup.groupby(col_keys)[['ipo_date']].min().reset_index()
        
        # We merge both to have oldest ipo_date and newest delisting_date
        data_dedup = (
            data_dup_first
            .drop(columns='ipo_date')
            .merge(data_dup_ipoDate,
                left_on=col_keys,
                right_on=col_keys)
        )
        
        # We combine rows with no duplicates with the rest of the rows now deduplicated
        data_clean = data_no_dup[final_cols].append(data_dedup[final_cols]).reset_index(drop=True)
        
        # Confirm that col_keys elements can be used as primary keys
        assert data_clean.drop_duplicates(subset=col_keys, keep=False).shape[0] == data_clean.shape[0]
        
        return data_clean


    def get_assets_table(self, data_clean):
        
        assets = (
            data_clean
            .loc[data_clean.asset_type == 'Stock']
            .sort_values(by=['symbol', 'name', 'asset_type'])
            .reset_index(drop=True)
            .reset_index()
            .rename(columns={'index': 'pid'})
        )
        
        return assets


    def refresh_listings(self,
                        date_input=None, 
                        assets_table='assets',
                        assets_alpha_table='assets_alpha', 
                        assets_alpha_clean_table='assets_alpha_clean'):

        # Date
        if date_input is None:
            date_input = datetime.now()
        
        # Download listing status
        data = self.download_all_listings(date_input)
        
        # Dedup listings
        #data_clean = self.clean_listings(data)
        
        # Assets table
        #assets = self.get_assets_table(data_clean)
        
        # Update assets_table
        self.sql.clean_table(assets_alpha_table)
        self.sql.upload_df_chunks(assets_alpha_table, data)
        
        # Update assets_table_clean
        #self.sql.clean_table(assets_alpha_clean_table)
        #self.sql.upload_df_chunks(assets_alpha_clean_table, data_clean)
        
        # Update assets
        #self.sql.clean_table(assets_table)
        #self.sql.upload_df_chunks(assets_table, assets)


    def get_adjusted_prices(self, symbol, size='full'):

        url = f'{URL_BASE}TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize={size}&apikey={self.api_key}'

        try:
            
            # Hit API
            r = requests.get(url)
            prices_json = r.json()

            # Transform into formatted pandas DataFrame
            if 'Time Series (Daily)' in prices_json:

                prices = pd.DataFrame(prices_json['Time Series (Daily)'], dtype='float').T

                # Remove enumeration at beginning of columns (e.g. "1. open")
                col_rename = {col: col[3:].replace(' ', '_') for col in prices.columns}
                prices.rename(columns=col_rename, inplace=True)

                # Date as a column
                prices = prices.reset_index().rename(columns={'index': 'date'})

                # Include symbol and put as first column
                prices['symbol'] = symbol
                cols = list(prices.columns[-1:]) + list(prices.columns[:-1])
                prices = prices[cols]

                # Change to appropriate variable types
                dtypes = {
                    'symbol': 'object',
                    'date': 'datetime64[ns]',
                }
                prices = prices.astype(dtypes)
                prices['date'] = prices.date.dt.date

                # Last update date
                prices['lud'] = datetime.now()                

                # Compute percentage return
                #prices = prices.sort_values(['symbol', 'date']).reset_index(drop=True)
                #prices['return'] = prices.groupby('symbol').adjusted_close.pct_change()

                return prices
            
            raise ValueError(f"Json file did not have 'Time Series (Daily)' as key. File content is:\n {prices_json}")

        except Exception as e:
            print(f'Download failed for {symbol}. \nurl: {url}')
            print(e)


    def get_db_prices(self, symbol):
        query = f"select * from prices_alpha where symbol = '{symbol}'"
        db_prices = self.sql.select_query(query)
        return db_prices
    

    def find_date_to_upload_from(self, api_prices, db_prices):
        """
            Returns date from which api_prices should be uploaded (inclusive)
            It is also the date up to which db dates are valid and should be kept
            If output is None, no filter should be applied over api_prices
            and all registers of the symbol should be deleted from the prices table
        """

        should_upload = False
        date_upload = None

        # Join API and database dates available
        api_dates_df = api_prices[['date', 'lud']].rename(columns={'lud': 'lud_api'})
        db_dates_df = db_prices[['date', 'lud']].rename(columns={'lud': 'lud_db'})
        dates_df = api_dates_df.merge(
            db_dates_df,
            how='outer',
            left_on='date',
            right_on='date'
        )

        # Get dates missing in db
        db_dates_missing = dates_df[dates_df.lud_db.isnull()].date

        if len(db_dates_missing) != 0:
        
            # Get dates in db that are potentially valid
            db_dates_valid = db_prices.loc[db_prices.date < db_dates_missing.min()]

            if db_dates_valid.shape[0] > 0:
                # Database has data for dates that are valid if no split or dividend
                # has ocurred since. To be validated later.
                should_upload = True
                date_upload = db_dates_valid.date.max()

            else:
                # Database has no useful dates. It should be updated entirely.
                should_upload = True
                date_upload = None
        
        else:
            # No dates missing. Database up to date
            should_upload = False
            date_upload = None
        
        return should_upload, date_upload




    def get_api_prices_to_upload(self, api_prices_input, db_prices, size):

        # Check and copy api_prices_input
        assert api_prices_input.shape[0] > 0
        api_prices = api_prices_input.copy()

        # Symbol
        api_symbols = api_prices.symbol.unique()
        db_symbols = db_prices.symbol.unique()
        assert len(api_symbols) == 1, f"Only one symbol in api_prices per update. symbols={api_symbols}"
        assert len(db_symbols) <= 1, f"At most one symbol in db_prices per update. symbols={db_symbols}"
        if len(db_symbols) == 1:
            assert db_symbols == api_symbols

        # Get last valid date of symbol in database (inclusive)
        # Which is also the same from which api_prices should be uploaded (inclusive)
        should_upload, date_upload = self.find_date_to_upload_from(api_prices, db_prices)

        # clean_db_table is True if the table has to be cleaned of symbol data
        clean_db_table = False

        if not should_upload:

            # There is nothing to upload
            api_prices = None
            
            # Returning False, None
            return should_upload, clean_db_table, api_prices

        if date_upload is None:

            # There are no valid dates in the db for the symbol.
            # Any rows related to the symbol should be erased
            # and the full history should be uploaded

            clean_db_table = True

            if size != 'full':
                # If the API data collected is not the full history
                # return False, meaning the upload has to be repeated
                # with the full history
                api_prices = None
            
            return should_upload, clean_db_table, api_prices

        else:
            # There is data in the db that is potentially useful

            # Check if prices and coefficients are the same in date_upload
            cols_equal = ['symbol', 'date', 'close', 'adjusted_close', 'dividend_amount', 'split_coefficient']
            db_cond = db_prices.date == date_upload
            api_cond = api_prices.date == date_upload
            db_prices_row = db_prices.loc[db_cond, cols_equal].reset_index(drop=True)
            api_prices_row = api_prices.loc[api_cond, cols_equal].reset_index(drop=True)
            
            if db_prices_row.equals(api_prices_row):
                # There is perfect continuation of the prices and coefficients
                # I.e. there was no split or dividend that would require
                # a retrospective adjustment of prices
                api_prices = api_prices.loc[api_prices['date'] > date_upload]
            
            else:
                # There was a split or dividend
                # All history should be downloaded

                clean_db_table = True

                if size != 'full':
                    api_prices = None
                
                return should_upload, clean_db_table, api_prices

        return should_upload, clean_db_table, api_prices


    def update_prices_symbol(self, symbol, size, table_prices='prices_alpha'):

        # Get API prices
        api_prices = self.get_adjusted_prices(symbol, size)

        # Get database prices
        db_prices = self.get_db_prices(symbol)

        # Check whether and what to upload
        should_upload, clean_db_table, api_prices = \
            self.get_api_prices_to_upload(api_prices, db_prices, size)

        if should_upload:

            if api_prices is None:
                # Fetch full history
                api_prices = self.get_adjusted_prices(symbol, size='full')
            
            if clean_db_table:
                # DB information has to be deleted for symbol

                # Clean symbol rows
                query = f"delete from {table_prices} where symbol = '{symbol}'"
                self.sql.query(query)

            # Upload to database
            assert api_prices.shape[0] > 0
            print(f'Uploading {api_prices.shape[0]} dates for {symbol}')
            self.sql.upload_df_chunks(table_prices, api_prices)

        else:

            print("Database already up to date.")
