
import os
import requests
import json
import pandas as pd
from datetime import datetime

from ..utils import sql_manager


# Get API Key
f = open('keys.json',)
keys = json.load(f)
api_key = keys["nyt"]["Key"]
now = datetime.now()


def nyt_api_download_month_articles(year, month):
    url = f"https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={api_key}"
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
    return biz_articles
