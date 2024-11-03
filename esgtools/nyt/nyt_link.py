
# External
import os
import sys
import requests
import json
import time
import pandas as pd
from datetime import datetime
import traceback

# Internal
from utils import sql_manager


# Manual mapping of NYT orgs to asset ids
# needed for stocks that are not easily linked by name
# or where name is not available
MANUAL_ORG_TO_STOCK_MAP = [
    ["Google Inc", ["GOOG-14542","GOOGL-90319"]],
    ["YouTube.com", ["GOOG-14542","GOOGL-90319"]],
    ["Facebook Inc", ["META-13407"]],
    ["Facebook.com", ["META-13407"]],
    ["Lehman Brothers Holdings Inc", ["LEH-80599"]],
    ["Wal-Mart Stores Inc", ["WMT-55976"]],
    ["Tesla Motors Inc", ["TSLA-93436"]],
    ["Tesla Motors", ["TSLA-93436"]],
    ["Bear Stearns Cos", ["BSC-68304"]],
    ["Bear Stearns Companies Incorporated", ["BSC-68304"]],
]


class NytNewsLinker:
    """ Linking NYT news to assets"""

    def __init__(self, org_to_asset_table: str, news_to_asset_table: str, db_credentials):
        # Initial parameters
        self.org_to_asset_table = org_to_asset_table
        self.news_to_asset_table = news_to_asset_table
        self.sql_manager = sql_manager.ManagerSQL(db_credentials)

    def update_news_links(self):

        # Read stock info
        stocks = self.sql_manager.select_query("""
            select id as asset_id, symbol, name, share_class, exchange, ipo_date, delisting_date
            from assets where asset_type = 'Stock'
        """)

        # Read news info
        nyt_news = self.sql_manager.select_query(f"select * from nyt_archive")

        # Get list of unique NYT organizations with their frequency on news articles
        org_counts = self.get_org_counts(nyt_news)

        # Base link between orgs and stocks based on name
        org_merged_base = self.get_org_to_asset_link_based_on_name(org_counts, stocks)

        # Link based on manual map
        org_merged_add = self.get_org_to_asset_link_based_on_manual_map(org_counts, stocks)

        # Final link between orgs and stocks
        org_merged = self.consolidate_org_to_asset_links(org_merged_base, org_merged_add)

        # Upload org to stock map
        self.upload_org_to_asset(org_merged)

        # Link news to stocks
        news_to_asset = self.get_news_to_asset_link(nyt_news, org_merged)

        # Upload news to stock map
        self.upload_news_to_asset(news_to_asset)

    @staticmethod
    def get_org_counts(nyt_news):
        exploded_orgs = nyt_news.organizations.str.split(' \| ').explode()
        org_counts = (
            exploded_orgs
            .value_counts()
            .reset_index()
            .rename(columns={"index": "organization", "organizations": "count"})
        )
        return org_counts
    
    def get_org_to_asset_link_based_on_name(self, org_counts: pd.DataFrame, stocks: pd.DataFrame):
        """ Systematic attempt at linking orgs with stocks based on a minimal version of the name. """
        col_clean = "name_clean"
        self.add_name_clean(org_counts, "organization", col_clean)
        self.add_name_clean(stocks, "name", col_clean)
        org_merged_base = org_counts.merge(
            stocks,
            how="left",
            left_on=[col_clean],
            right_on=[col_clean]
        )
        return org_merged_base

    @staticmethod
    def add_name_clean(df: pd.DataFrame, col: str, col_clean: str):
        df[col_clean] = (
            df[col]
            .str.lower()
            .str.replace(r"[.,&+()!`']", "", regex=True)
            .str.replace(r"\b(corporation|company|incorporated|group|stores|enterprise|enterprises|holdings|solutions|technology|technologies|pictures|television|entertainment|network|limited|corp|co|inc|ltd|llc|plc|ag|sa|spa|nv|the)\b", "", regex=True)
            .str.replace(r"[/]", "", regex=True)
            .str.replace(r"class\s+[a-z]$", "", regex=True)
            .str.replace(r"cls\s+[a-z]$", "", regex=True)
            .str.replace(r"\-", " ", regex=True)
            .str.strip()
        )

    @staticmethod
    def get_org_to_asset_link_based_on_manual_map(org_counts: pd.DataFrame, stocks: pd.DataFrame):

        # Prepare map
        df_manual_org_to_stock_map = (
            pd.DataFrame(
                MANUAL_ORG_TO_STOCK_MAP,
                columns=["organization", "asset_id"]
            )
        )
        df_manual_org_to_stock_map = (
            df_manual_org_to_stock_map
            .explode("asset_id")
        )

        # Merge with manual map
        org_merged_add = (
            org_counts.merge(
                df_manual_org_to_stock_map,
                how="left",
                left_on=["organization"],
                right_on=["organization"]
            )
        )
        org_merged_add = (
            org_merged_add
            .loc[~org_merged_add.asset_id.isnull()]
        )
        org_merged_add = (
            org_merged_add
            .merge(
                stocks.rename(columns={"name_clean": "name_clean_manual"}),
                how="left",
                left_on=["asset_id"],
                right_on=["asset_id"]
            )
        )

        return org_merged_add

    @staticmethod
    def consolidate_org_to_asset_links(org_merged_base: pd.DataFrame, org_merged_add: pd.DataFrame):

        # Merge between both strategies
        org_merged_raw = (
            pd.concat((org_merged_base,
                    org_merged_add[org_merged_base.columns]), 
                    axis=0)
        )

        # Separate those with missing link
        org_merged = (
            org_merged_raw
            .drop_duplicates(["organization", "asset_id"])
            .groupby('organization').agg({
                'asset_id': lambda x: ','.join(x.dropna()),
                'count': 'first'
            })
            .rename(columns={"asset_id": "asset_ids"})
            .reset_index()
            .sort_values(["count"], ascending=False)
        )

        return org_merged

    @staticmethod
    def get_news_to_asset_link(nyt_news, org_merged) -> pd.DataFrame:

        # Explode orgs column in news
        nyt_news_expanded = (
            nyt_news
            .assign(organizations=nyt_news['organizations'].str.split(' \| '))
            .explode('organizations')
            .reset_index(drop=True)
            .rename(columns={"organizations": "organization"})
        )

        news_to_asset = (
            nyt_news_expanded[["web_url", "organization"]]
            .merge(
                org_merged[["organization", "asset_ids"]],
                how="left",
                on=["organization"]
            )
            .drop_duplicates(["web_url", "organization"], keep="first", inplace=True)
        )

        return news_to_asset

    def upload_org_to_asset(self, org_merged: pd.DataFrame):
        org_to_asset_cols = ["organization", "count", "asset_ids"]
        self.sql_manager.query(f"delete from {self.org_to_asset_table}")
        self.sql_manager.upload_df_chunks(self.org_to_asset_table, org_merged[org_to_asset_cols])

    def upload_news_to_asset(self, news_to_asset: pd.DataFrame):
        news_to_asset_cols = ["web_url", "organization", "asset_ids"]
        self.sql_manager.query(f"delete from {self.news_to_asset_table}")
        self.sql_manager.upload_df_chunks(self.news_to_asset_table, news_to_asset[news_to_asset_cols])
