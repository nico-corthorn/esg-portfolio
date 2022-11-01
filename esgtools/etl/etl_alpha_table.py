

import os
import csv
import requests
import time
import pandas as pd
from datetime import date, datetime
from abc import ABC , abstractmethod
from pandas.tseries.offsets import BDay
from pytz import timezone

from esgtools.utils import sql_manager, utils

URL_BASE = 'https://www.alphavantage.co/query?function='

class AlphaTable(ABC):

    def __init__(self, table_name, columns, primary_keys, connect=True):
        self.table_name = table_name
        self.columns = columns
        self.primary_keys = primary_keys
        if connect:
            self.sql = sql_manager.ManagerSQL()


    @abstractmethod
    def update_all(self, *args, **kwargs):
        # refresh
        pass


    def update(self, *args, **kwargs):
        # update_symbol
        pass


    @abstractmethod
    def get_api_data(self):
        pass
    

    def _get_db_data(self, symbol: str) -> pd.DataFrame:
        query = f"select * from {self.table_name} where symbol = '{symbol}'"
        db_data = self.sql.select_query(query)
        return db_data


class AlphaTableAssets(AlphaTable):
     
    def update_all(self, date_input=None):

        """Updates assets_alpha table as of date_input with listed and delisted
        stocks and ETFs.

            Parameters
            ----------
            date_input: datetime.datetime
                Date to use for API call of listed and delisted
            assets_alpha_table: str
                Name of assets table. Default should be kept in most cases

            Returns
            -------
            None

            Side effects
            ------------
            Deletes assets_alpha content and then updates the table
            with data from the API using date_input
        """

        # Date
        if date_input is None:
            date_input = datetime.now()
        
        # Download listing status
        data = self.get_api_data(date_input)
                
        # Update assets_table
        self.sql.clean_table(self.table_name)
        self.sql.upload_df_chunks(self.table_name, data)

    # download_all_listings (delete in etl_alpha)
    def get_api_data(self, date_input):
        
        # Download active
        data_active = self._download_active_listings()
        
        # Download delisted
        data_delist = self._download_delisted(date_input)
        
        # Concatenate
        data = data_active.append(data_delist).reset_index(drop=True)

        # Rename columns
        data.columns = [utils.camel_to_snake(col) for col in data.columns]
        
        # Fix missing values in delisting_date column
        data.loc[data.delisting_date == 'null', 'delisting_date'] = None
        
        return data


    def _download_active_listings(self) -> pd.DataFrame:
        """Hit AlphaVantage API to get active listings

            Returns
            -------
            pd.DataFrame
                DataFrame with listings that are active
        """
        
        url = f'{URL_BASE}LISTING_STATUS&apikey=demo'
        
        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            data = pd.DataFrame(my_list[1:], columns=my_list[0])
        
        return data


    def _download_delisted(self, date_input: datetime) -> pd.DataFrame:
        """Hit AlphaVantage API to get delisted assets

            Parameters
            ----------
            date_input : datetime
                Date at which the delisting snapshot is taken

            Returns
            -------
            pd.DataFrame
                DataFrame with listings that were delisted as of given date

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


class AlphaTablePrices(AlphaTable):

    # refresh_all_prices (delete in etl_alpha)
    def update_all(
        self, 
        size:str = 'full',
        validate:bool = False, 
        asset_types:list = ['Stock']
        ):
        """Tries to update prices_alpha table as of latest closed availables for each assets in
        assets_alpha table. Each symbol is ran in parallel

            Parameters
            ----------
            size: str
                Either 'compact' or 'full'. If compact, API returns latest 100 records.
                If full it returns all available records
            validate: bool
                If False, it will try to update only assets that are up to date
                according to today's date or their delisting date. If True, it will
                try to update all assets in assets_alpha
            asset_types: list
                Asset types to consider for update. Asset types not in the list
                will not be considered. Reasonable options are ['Stock'] or ['Stock', 'ETF']

            Returns
            -------
            None

            Side effects
            ------------
            Updates prices_alpha table. It tries to keep previous records if nothing
            has changed. It will update the whole history of symbols that have had
            a price event that adjusts retroactively likely splits and dividends.
        """

        # Get available assets from db
        assets = self.get_assets_to_refresh(asset_types, validate)
        if not validate:
            assets = self._keep_only_incomplete_per_prices_alpha(assets)

        # Update db prices in parallel
        args = [(symbol, size) for symbol in assets.symbol]
        fun = lambda p: self.update(*p)
        utils.compute(args, fun, max_workers=self.max_workers)
        #utils.compute_loop(args, fun)  # temporal, for debugging purposes


    # update_prices_symbol (delete in etl_alpha)
    def update(self, symbol, size):

        print(f'Updating prices for {symbol}')

        # Get API prices
        api_prices = self.get_adjusted_prices(symbol, size)

        if api_prices is not None:

            if api_prices.shape[0] > 0:

                # Get database prices
                db_prices = self._get_db_data(symbol, self.table_name)

                # Check whether and what to upload
                should_upload, clean_db_table, api_prices = \
                    self._get_api_prices_to_upload(api_prices, db_prices, size)

                if should_upload:

                    if api_prices.empty:
                        # Fetch full history
                        api_prices = self.get_adjusted_prices(symbol, size='full')
                    
                    if clean_db_table:
                        # DB information has to be deleted for symbol

                        # Clean symbol rows
                        query = f"delete from {self.table_name} where symbol = '{symbol}'"
                        self.sql.query(query)
                    else:
                        date_min = api_prices.date.min()
                        query = f"""
                        delete from {self.table_name} 
                        where symbol = '{symbol}'
                            and date >= '{date_min}'
                        """
                        self.sql.query(query)

                    # Upload to database
                    assert api_prices.shape[0] > 0
                    print(f'Uploading {api_prices.shape[0]} dates for {symbol}')
                    self.sql.upload_df_chunks(self.table_name, api_prices)

                else:

                    print(f'Database already up to date for {symbol}')



#class AlphaTableBalance(AlphaTable):

#class AlphaTableIncome(AlphaTable):


