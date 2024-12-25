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
    def __init__(self, db_url: str = "sqlite:///crypto_data.db", api_key: Optional[str] = None):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.api_key = api_key
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
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}

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
                headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}

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
    

    def save_to_database(self, crypto_data: Dict) -> None:
        """
        Save cryptocurrency data to database.
        
        Args:
            crypto_data: Dictionary containing cryptocurrency price data
        """
        Session = sessionmaker(bind=self.engine)
        session = Session()

        try:
            for crypto_id, df in crypto_data.items():
                records = []
                for timestamp, row in df.iterrows():
                    record = {
                        'timestamp': timestamp,
                        'crypto_id': crypto_id,
                        'price': row['price'],
                        'market_cap': row['market_cap'],
                        'volume': row['volume']
                    }
                    records.append(record)

                if records:
                    session.execute(
                        self.prices_table.insert().prefix_with('OR REPLACE'),
                        records
                    )

            session.commit()
            self.logger.info(f"Successfully saved data for {len(crypto_data)} cryptocurrencies")

        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Database error: {str(e)}")
        finally:
            session.close()


    def get_crypto_data(
        self,
        crypto_ids: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Retrieve cryptocurrency data from database.
        
        Args:
            crypto_ids: List of cryptocurrency IDs (None for all)
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            DataFrame containing price data
        """
        query: f"SELECT * FROM crypto_prices"
        conditions = []

        if crypto_ids:
            crypto_list = ", ".join(f"'{id}'" for id in crypto_ids)
            conditions.append(f"crypto_id IN ({crypto_list})")

        if start_date:
            conditions.append(f"timestamp >= '{start_date}'")
        
        if end_date:
            conditions.append(f"timestamp >= '{end_date}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        try:
            df = pd.read_sql(query, self.engine)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            return df

        except Exception as e:
            self.logger.error(f"Error retrieving data from database: {str(e)}")
            return pd.DataFrame()
    

    def update_all_crypto_data(self, top_n: int = 100, days: str = "max") -> None:
        """
        Update database with latest data for top N cryptocurrencies.
        
        Args:
            top_n: Number of top cryptocurrencies to fetch
            days: Number of days of historical data to fetch
        """

        coins = self.get_available_cryptocurrencies(per_page=top_n)
        crypto_ids = [coin['id'] for coin in coins]

        crypto_data = self.fetch_crypto_prices(crypto_ids, days=days)
        self.save_to_database(crypto_data)

        self.update_metadata(coins)

    
    def update_metadata(self, coins: List[Dict]) -> None:
        """
        Update cryptocurrency metadata in database.
        
        Args:
            coins: List of cryptocurrency dictionaries
        """
        Session = sessionmaker(bind=self.engine)
        session = Session()

        try:
            for coin in coins:
                record = {
                    'crypto_id': coin['id'],
                    'name': coin['name'],
                    'symbol': coin['symbol'],
                    'last_updated': datetime.now()
                }
                
                session.execute(
                    self.metadata_table.insert().prefix_with('OR REPLACE'),
                    record
                )
                
            session.commit()
            self.logger.info(f"Successfully updated metadata for {len(coins)} cryptocurrencies")
            
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Database error updating metadata: {str(e)}")
        finally:
            session.close()


        # Example usage
if __name__ == "__main__":
    collector = CryptoDataCollector()
    
    # Collect data for top 100 Cryptocurrencies 
    collector.update_all_crypto_data(top_n=10)
    
    # Get specific crypto data
    crypto_data = collector.get_crypto_data(
        crypto_ids=['bitcoin', 'ethereum', 'doge'],
        start_date=datetime.now() - timedelta(days=30)
    )

    print(crypto_data.head())