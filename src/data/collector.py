import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
import os
from typing import List, Dict, Optional
from sqlalchemy import create_engine, Table, Column, Float, DateTime, String, MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

class CryptoDataCollector:
    def __init__(self, db_url: str = "sqlite:///crypto_data.db"):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.engine = create_engine(db_url)
        self.setup_logging()
        self.setup_database()



    def setup_logging(self):
        """
        Configure logging for the data collector
        """
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('crypto_collector.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    

    def setup_database(self):
        metadata = MetaData()

        # Table for storing crypto price data
        self.prices_table = Table(
            'crypto_prices', 
            metadata,
            Column('timestamp', DateTime, primary_key=True),
            Column('crypto_id', String, primary_key=True),
            Column('price', Float),
            Column('market_cap', Float),
            Column('volume', Float)
        )
        
        # Table for storing crypto metadata
        self.metadata_table = Table(
            'crypto_metadata',
            metadata,
            Column('crypto_id', String, primary_key=True),
            Column('name', String),
            Column('symbol', String),
            Column('last_updated', DateTime)
        )

        metadata.create_all(self.engine)


    def get_available_cryptocurrencies(self, page: int = 1, per_page: int = 250) -> List[Dict]:
        """
        Fetch list of available cryptocurrencies from CoinGecko.
        
        Args:
            page: Page number for pagination
            per_page: Number of results per page
            
        Returns:
            List of cryptocurrency dictionaries containing id, symbol, and name
        """
        try:
            endpoint = f"{self.base_url}/coins/markets"
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": per_page,
                "page": page,
                "sparkline": False
            }

            response = requests.get(endpoint, params=params)
            response.raise_for_status()

            coins = response.json()
            self.logger.info(f"successfully fetched {len(coins)} cryptocurrencies")
            return coins
        
        except Exception as e:
            self.logger.error(f"Error fetching available cryptocurrencies: {str(e)}")
            return []
        

    def fetch_crypto_prices(self, crypto_ids: List[str], days: str = "max") -> Dict:
        """
        Fetch historical price data for specified cryptocurrencies.
        
        Args:
            crypto_ids: List of cryptocurrency IDs (e.g., ['bitcoin', 'ethereum'])
            days: Number of days of historical data to fetch
        
        Returns:
            Dictionary containing price data for each cryptocurrency
        """

        all_crypto_data = {}

        for crypto_id in crypto_ids:
            try:
                endpoint = f"{self.base_url}/coins/{crypto_id}/market_chart"
                params = {
                    "vs_currency": "usd",
                    "days": days,
                    "interval": "daily"
                }

                response = requests.get(endpoint, params=params)
                response.raise_for_status()
                data = response.json()

                # Create DataFrame for all available data
                df = pd.DataFrame(index=pd.to_datetime([x[0] for x in data["prices"]], unit="ms"))
                df["price"] = [x[1] for x in data["prices"]]
                df["market_cap"] = [x[1] for x in data["market_caps"]]
                df["volume"] = [x[1] for x in data["total_volumes"]]
                df["crypto_id"] = crypto_id

                all_crypto_data[crypto_id] = df

                time.sleep(1.5)
                self.logger.info(f"Successfully fetched data for {crypto_id}")

            except Exception as e:
                self.logger.error(f"Error fetching data for {crypto_id}: {str(e)}")
                continue

        return all_crypto_data
    

    
        

        # Example usage
if __name__ == "__main__":
    collector = CryptoDataCollector()
    
    # Collect data for top 100 Cryptocurrencies 
    collector.update_all_crypto_data(top_n=100)
    
    # Get specific crypto data
    btc_eth_data = collector.get_crypto_data(
        crypto_ids=['bitcoin', 'ethereum'],
        start_date=datetime.now() - timedelta(days=30)
    )

    print(btc_eth_data.head())