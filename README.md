# Introduction

This NLP application will:
- Classify articles into topics using both supervised and unsupervised learning. ESG topics are used for the supervised approach. New York Times historical articles data is used. 
- Determine the sentiment of the article. A labeled Kagle data set is used to train this model. Almost 5,000 financial headlines are used.
- Compute signals for portfolio construction using both the topics and the sentiment of the articles for public companies.
- Build portfolios using this signals. Stock prices data from Tiingo are used for the construction and backtest. Focus on largest 500 U.S. companies.

# Data

Information stored in AWS RDS.

### New York Times

More than 80,000 articles were collected from the New York Times API. Articles starting from 2001. Only business section was considered. Collected the headline, snippet, subjects, and organizations of each article. Data available in data/nyt.csv. Code used available in code/NYT_scraper.ipynb.

Source: https://developer.nytimes.com/

### Sentiment Labeled Financial Headlines

Almost 5,000 financial headlines were downloaded from Kaggle. Data available in the data folder (data/sentiment_news/all-data.csv).

Source: https://www.kaggle.com/ankurzing/sentiment-analysis-for-financial-news

### Stock Prices

Stock price information of all supported stocks from Alpha Vantage API.

Source: https://www.alphavantage.co/documentation/
