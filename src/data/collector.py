import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from typing import List, Dict
import logging

class CryptoDataCollector:
    def __init__(self, base_path: str = "data"):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.base_path = base_path
        self.setup_logging()

    # Create data directory if it doesn't exist
        if not os.path.exists(base_path):
            os.makedirs(base_path)


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

                # Convert to DataFrame
                df = pd.DataFrame(data["prices"], columns=["timestamp", "price"])
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                df.set_index("timestamp", inplace=True)

                all_crypto_data[crypto_id] = df

                time.sleep(1.5)

                self.logger.info(f"Successfully fetched data for {crypto_id}")

            except Exception as e:
                self.logger.error(f"Error fetching data for {crypto_id}: {str(e)}")
                continue

        return all_crypto_data
    

    def save_to_csv(self, crypto_data: Dict, suffix: str = "") -> None:
        """
        Save cryptocurrency data to CSV files.
        
        Args:
            crypto_data: Dictionary containing cryptocurrency price data
            suffix: Optional suffix for filename
        """

        for crypto_id, df in crypto_data.items():
            filename = f"{crypto_id}_prices{suffix}.csv"
            filepath = os.path.join(self.base_path, filename)

            try:
                df.to_csv(filepath)
                self.logger.info(f"Successfully saved data to {filepath}")
            except Exception as e:
                self.logger.error(f"Error saving data for {crypto_id}: {str(e)}")

    def load_from_csv(self, crypto_id: str, suffix: str = "") -> pd.DataFrame:
        """
        Load cryptocurrency data from CSV file.
        
        Args:
            crypto_id: Cryptocurrency ID
            suffix: Optional suffix for filename
        
        Returns:
            DataFrame containing price data
        """
        filename = f"{crypto_id}_prices{suffix}.csv"
        filepath = os.path.join(self.base_path, filename)

        try:
            df = pd.read_csv(filepath, index_col=0, parse_dates=True)
            self.logger.info(f"Successfully loaded data from {filepath}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading data for {crypto_id}: {str(e)}")
            return None
        

        # Example usage
if __name__ == "__main__":
    collector = CryptoDataCollector()
    
    # Fetch data for Bitcoin and Ethereum
    crypto_ids = ["bitcoin", "ethereum"]
    crypto_data = collector.fetch_crypto_prices(crypto_ids, days="365")
    
    # Save to CSV
    collector.save_to_csv(crypto_data)
    
    # Load Bitcoin data
    btc_data = collector.load_from_csv("bitcoin")
    print(btc_data.head())