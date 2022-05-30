import os
import time

from coinmarketcapapi import CoinMarketCapAPI
from coinmarketcapapi import Response
from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy import create_engine
import sqlalchemy
load_dotenv()

cmc = CoinMarketCapAPI(os.getenv('COINMARKET_CAP_KEY'))

engine = create_engine(
    sqlalchemy.engine.url.URL(
        drivername='postgresql+pg8000',
        username=os.getenv('GCP_DB_USER'),
        password=os.getenv('GCP_DB_PASSWORD'),        
        database=os.getenv('GCP_DB_NAME'),
        query={
            "unix_sock": "{}/{}/.s.PGSQL.5432".format(
                "/cloudsql",  # e.g. "/cloudsql"
                os.getenv('GCP_DB_INSTANCE_NAME'))  # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
        }
    ))

def get_cryptocurrency_listings_latest(cryptocurrency_type="coins", limit=12, convert="USD"):
    r: Response= cmc.cryptocurrency_listings_latest(cryptocurrency_type=cryptocurrency_type, limit=limit, convert=convert)
    return r.data

def get_parsed_cryptocurrency_list(data):
    summary = []
    for a in data:
        summary.append({
            "name": a['name'],
            "symbol": a['symbol'],
            "price": a['quote']['USD']['price'],
            "market_cap": a['quote']['USD']['market_cap'],
            "volume_24h": a['quote']['USD']['volume_24h'],
            "percent_change_1h": a['quote']['USD']['percent_change_1h'],
            "percent_change_24h": a['quote']['USD']['percent_change_24h'],
            "percent_change_7d": a['quote']['USD']['percent_change_7d'],
            "weight": 0,
            "date": time.time()
        })
    return summary

def get_weighted_cryptocurrency_list(data):
    total_market_cap = 0
    for a in data:
        total_market_cap += a['market_cap']

    for a in data:
        a['weight'] = a['market_cap'] / total_market_cap
        
    return DataFrame(data)    

# functions_framework --target=main
import functions_framework
@functions_framework.http
def main(request):
    data = get_cryptocurrency_listings_latest() # From CoinMarketCapAPI
    data = get_parsed_cryptocurrency_list(data) # Summary of data
    data = get_weighted_cryptocurrency_list(data) # Weighted data for each coin
    data.to_sql('CMK12', con=engine, if_exists='append', index=False)
    return data.to_json()

