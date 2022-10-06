
import os
import csv
import requests
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import BDay

from utils import sql_manager, utils

class AlphaScraper():

    def __init__(self):
        self.sql = sql_manager.ManagerSQL()
        self.today = datetime.now().date()
        self.last_business_date = (self.today - BDay(1)).date()
        self.min_date = pd.to_datetime('1960-01-01').date()
        self.api_key = os.environ.get('ALPHAVANTAGE_API_KEY')


    def download_active_listings(self):
        
        url = f'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo'
        
        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            data = pd.DataFrame(my_list[1:], columns=my_list[0])
        
        return data


    def download_delisted(self, date_input, key):
        """
            Date is datetime.datetime or datetime.date
        """
        
        dte = date_input.strftime("%Y-%m-%d")
        
        url = f'https://www.alphavantage.co/query?function=LISTING_STATUS&date={dte}&state=delisted&apikey={key}'

        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            data = pd.DataFrame(my_list[1:], columns=my_list[0])
        
        return data
    

    def download_all_listings(self, date_input):
        
        # download active
        data_active = self.download_active_listings()
        
        # download delisted
        data_delist = self.download_delisted(date_input, self.api_key)
        
        # concatenate
        data = data_active.append(data_delist).reset_index(drop=True)

        # rename columns
        data.columns = [utils.camel_to_snake(col) for col in data.columns]
        
        # fix missing values in delisting_date column
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

        # date
        if date_input is None:
            date_input = datetime.now()
        
        # download listing status
        data = self.download_all_listings(date_input)
        
        # dedup listings
        data_clean = self.clean_listings(data)
        
        # assets table
        assets = self.get_assets_table(data_clean)
        
        # update assets_table
        self.sql.clean_table(assets_alpha_table)
        self.sql.upload_df_chunks(assets_alpha_table, data)
        
        # update assets_table_clean
        self.sql.clean_table(assets_alpha_clean_table)
        self.sql.upload_df_chunks(assets_alpha_clean_table, data_clean)
        
        # update assets
        self.sql.clean_table(assets_table)
        self.sql.upload_df_chunks(assets_table, assets)

