
import os
import csv
import requests
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import BDay
from pytz import timezone

from esgtools.utils import sql_manager, utils

URL_BASE = 'https://www.alphavantage.co/query?function='


class AlphaScraper():

    def __init__(self, connect=True, asof=None):
        if connect:
            self.sql = sql_manager.ManagerSQL()
        if asof is None:
            asof = datetime.today()
        self.today = asof
        self.last_business_date = self._get_last_business_date()
        self.api_key = os.environ.get('ALPHAVANTAGE_API_KEY')


    def _get_last_business_date(self):
        """Returns last business date that has closed in NY
            
            Returns
            -------
            datetime.date
                Last business date that has closed in NY.
                If self.today is a weekend or holiday, it returns the last business date.
                If self.today is a business date, then it returns the same date if it is past
                4 pm ET, otherwise the last business date.
        """

        # Check whether it is a business day
        is_business_day = (self.today - BDay(1) + BDay(1)).date() == self.today.date()

        # Check whether stock market is closed in USA
        time_eastern = self.today.astimezone(timezone('US/Eastern'))
        is_after_4_pm = time_eastern.hour >= 16

        # If weekday and after 4 pm ET
        if is_business_day and is_after_4_pm:
            return self.today.date()

        # If weekend, holiday or before 4 pm then return last business day
        return (self.today.date() - BDay(1)).date()


    def refresh_listings(self,
                        date_input=None, 
                        assets_alpha_table='assets_alpha'):
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
        data = self.download_all_listings(date_input)
                
        # Update assets_table
        self.sql.clean_table(assets_alpha_table)
        self.sql.upload_df_chunks(assets_alpha_table, data)


    def refresh_all_prices(
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

        # Update db prices in parallel
        args = [(symbol, size) for symbol in assets.symbol]
        fun = lambda p: self.update_prices_symbol(*p)
        utils.compute(args, fun, max_workers=5)
        #utils.compute_loop(args, fun)  # temporal, for debugging purposes


    def get_assets_to_refresh(self, asset_types, validate):
        """ Returns DataFrame with unique tickers from asset_table
        """

        # Get full assets table
        query = f"select * from assets_alpha"
        assets = self.sql.select_query(query)

        # Filter and drop duplicates
        assets = (
            assets
            .loc[assets.asset_type.isin(asset_types)]
            .sort_values(by=['symbol', 'status', 'delisting_date'])
            .drop_duplicates(subset='symbol', keep='first')
            .reset_index(drop=True)
        )

        if not validate:

            # Get last available date in db prices table
            query = """
            select symbol, max(date) max_date
            from prices_alpha
            group by symbol
            """
            assets_max_date = self.sql.select_query(query)

            assets = assets.merge(
                assets_max_date,
                how = 'left',
                left_on='symbol',
                right_on='symbol'
            )

            # if active, last date in db (max_date) == self.last_business_date
            cond_last_bd = assets.max_date == self.last_business_date
            cond_delisted = assets.max_date >= assets.delisting_date
            cond_updated = cond_last_bd | cond_delisted
            assets = assets.loc[~cond_updated]
        
        return assets


    def download_all_listings(self, date_input):
        
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
    

    def update_prices_symbol(self, symbol, size, table_prices='prices_alpha'):

        print(f'Updating prices for {symbol}')

        # Get API prices
        api_prices = self.get_adjusted_prices(symbol, size)

        if api_prices is not None:

            if api_prices.shape[0] > 0:

                # Get database prices
                db_prices = self._get_db_prices(symbol)

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
                        query = f"delete from {table_prices} where symbol = '{symbol}'"
                        self.sql.query(query)
                    else:
                        date_min = api_prices.date.min()
                        query = f"""
                        delete from {table_prices} 
                        where symbol = '{symbol}'
                            and date >= '{date_min}'
                        """
                        self.sql.query(query)

                    # Upload to database
                    assert api_prices.shape[0] > 0
                    print(f'Uploading {api_prices.shape[0]} dates for {symbol}')
                    self.sql.upload_df_chunks(table_prices, api_prices)

                else:

                    print(f'Database already up to date for {symbol}')


    def get_adjusted_prices(self, symbol, size='full'):
        """Hit AlphaVantage API to get prices of symbol

                Parameters
                ----------
                date_input : datetime
                    Date at which the delisting snapshot is taken

                Returns
                -------
                pd.DataFrame or None
                    DataFrame with prices in API, according to given size
                    If it fails to retrieve prices successfully, returns None
        """

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


    def _get_db_prices(self, symbol: str) -> pd.DataFrame:
        query = f"select * from prices_alpha where symbol = '{symbol}'"
        db_prices = self.sql.select_query(query)
        return db_prices
    

    def _find_date_to_upload_from(
        self, api_prices: pd.DataFrame, db_prices: pd.DataFrame):
        """Returns whether the data from the API should be included in the db,
        and the maximum date to keep information in the database. Expecting
        only 1 symbol (unchecked).

            Parameters
            ----------
            api_prices : pd.DataFrame
                API data including symbol, close, adjusted_close, etc
            db_prices: pd.DataFrame
                Data from the database with the same columns as api_prices

            Returns
            -------
            should_upload: bool
                True if there is data in the API to upload for the symbol
            date_upload: datetime or None
                Maximum date to keep information in the database unless there
                was a price event.
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


    def _get_api_prices_to_upload(
        self, 
        api_prices: pd.DataFrame, 
        db_prices: pd.DataFrame, 
        size: str,
        ):
        """Returns whether the data from the API should be included in the db,
        whether to clean all the db data for the symbol and the API data to
        upload, if any.

            Parameters
            ----------
            api_prices : pd.DataFrame
                API data including symbol, close, adjusted_close, etc
            db_prices: pd.DataFrame
                Data from the database with the same columns as api_prices

            Returns
            -------
            should_upload: bool
                True if there is data in the API to upload for the symbol.
                Otherwise False
            clean_db_table: bool
                True if the prices_alpha table should be cleaned of the underlying
                symbol. Otherwise False
            api_prices: pd.DataFrame
                API data that can be directly uploaded to prices_alpha in the db
        """

        # Check and copy api_prices_input
        assert api_prices.shape[0] > 0

        # Symbol
        api_symbols = api_prices.symbol.unique()
        db_symbols = db_prices.symbol.unique()
        assert len(api_symbols) == 1, f"Only one symbol in api_prices per update. symbols={api_symbols}"
        assert len(db_symbols) <= 1, f"At most one symbol in db_prices per update. symbols={db_symbols}"
        if len(db_symbols) == 1:
            assert db_symbols == api_symbols

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

        # should_upload is True if there are API dates missing in db
        should_upload = False

        # clean_db_table is True if the table has to be cleaned of symbol data
        clean_db_table = False

        # Empty DataFrame
        empty_df = pd.DataFrame(columns=api_prices.columns)

        if len(db_dates_missing) == 0:

            # There is nothing to upload
            api_prices = empty_df
            
            # Returning False, False, Empty DataFrame
            return should_upload, clean_db_table, api_prices

        # There are API dates missing in the db
        should_upload = True

        # Get dates in db that are potentially valid
        db_dates_valid = db_prices.loc[db_prices.date < db_dates_missing.min()]

        if db_dates_valid.shape[0] == 0:

            # There are no valid dates in the db for the symbol.
            # Any rows related to the symbol should be erased
            # and the full history should be uploaded

            clean_db_table = True

            if size != 'full':
                # If the API data collected is not the full history
                # return False, meaning the upload has to be repeated
                # with the full history
                api_prices = empty_df
            
            return should_upload, clean_db_table, api_prices

        else:

            # Database has data for dates that are valid if no split or dividend
            # has ocurred since. To be validated later.

            # The date up to which db data might be kept is the following
            date_upload = db_dates_valid.date.max()

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
                    api_prices = empty_df
                
                return should_upload, clean_db_table, api_prices

        return should_upload, clean_db_table, api_prices


    ###Balance:

    def _download_balance(self, symbol):
        """Hit AlphaVantage API to get balance sheet

            Returns
            -------
            pd.DataFrame
                DataFrame with balance sheet
        """
        
        url = f'{URL_BASE}BALANCE_SHEET&symbol={symbol}&apikey={self.api_key}'
        r = requests.get(url)
        data_json = r.json()
        
        accounts = ['totalAssets', 'commonStock', 'commonStockSharesOutstanding']
        final_cols = ['symbol', 'report_type', 'report_date', 'currency', 'account_name', 'account_value']
        key_report_map = {
        'annualReports': 'A',
        'quarterlyReports': 'Q'}

        data_annual = self.wrangle_json(data_json, 'annualReports', key_report_map, accounts, final_cols)
        data_quarter = self.wrangle_json(data_json, 'quarterlyReports', key_report_map, accounts, final_cols)
        data = data_annual.append(data_quarter)

        return data


    def wrangle_json(self, symbol, data_json, key_report, key_report_map, accounts, final_cols):
        
        data = pd.DataFrame()

        for report in data_json[key_report]:
            
            data_report_lst = [[k, v] for k, v in report.items() if k in accounts]
            currency = report['reportedCurrency']
            fiscal_date = report['fiscalDateEnding']
            data_report = pd.DataFrame(data_report_lst, columns=['account_name', 'account_value'])
            data_report['symbol'] = symbol
            data_report['report_type'] = key_report_map[key_report]
            data_report['report_date'] = fiscal_date
            data_report['currency'] = currency
            data_report = data_report[final_cols]
            data = data.append(data_report)

        return data



