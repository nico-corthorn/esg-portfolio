import os
import sys
import json
import argparse
import logging
import traceback
from ast import literal_eval

import pandas as pd
from esgtools.utils import aws, sql_manager
from esgtools.domain_models.io import convert_dict_to_sql_params

# Create output directory if it doesn't exist
os.makedirs('/opt/ml/processing/output', exist_ok=True)

# Configure logging to write to stdout
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Clear any existing handlers
logger.handlers = []

# Create console handler with a higher log level
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

def parse_args():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--region', type=str, default=os.environ.get('AWS_DEFAULT_REGION'))
        args = parser.parse_args()
        logger.info(f"Args parsed successfully: {vars(args)}")
        return args
    except Exception as e:
        logger.error(f"Error parsing args: {str(e)}")
        raise

def fetch_and_prepare_data(region):
    try:
        logger.info("Starting fetch_and_prepare_data")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Current directory: {os.getcwd()}")
        logger.info(f"Directory contents: {os.listdir('.')}")
        logger.info(f"PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")
        
        # Get database credentials from Secrets Manager
        logger.info("Fetching database credentials")
        sql_params = convert_dict_to_sql_params(literal_eval(aws.get_secret("prod/awsportfolio/key")))
        logger.info("Successfully retrieved and converted database credentials")
        
        # Connect to database and fetch data
        logger.info("Connecting to database")
        sql = sql_manager.ManagerSQL(sql_params)
        
        year_month = "200605"
        
        logger.info(f"Fetching data for year_month: {year_month}")
        query = f"""
        SELECT headline, snippet 
        FROM nyt_archive 
        WHERE year_month = '{year_month}'
        """
        nyt_data = sql.select_query(query)
        nyt_data = nyt_data.head()
        logger.info(f"Retrieved {len(nyt_data)} records from database")
        
        inference_data = nyt_data[['headline', 'snippet']]
        
        # Save to output location as JSONL
        output_path = os.path.join('/opt/ml/processing/output', "data.jsonl")
        records_written = 0
        
        with open(output_path, 'w') as f:
            for _, row in inference_data.iterrows():
                f.write(json.dumps(row.to_dict()) + '\n')
                records_written += 1
                
        logger.info(f"Successfully wrote {records_written} records to {output_path}")
        
    except Exception as e:
        logger.error(f"Error in fetch_and_prepare_data: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    try:
        logger.info("Script started")
        args = parse_args()
        logger.info(f"Starting preprocessing script in region: {args.region}")
        fetch_and_prepare_data(args.region)
        logger.info("Preprocessing completed successfully")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)