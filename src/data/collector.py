import pandas as pd
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
from typing import Optional, List
from sqlalchemy import create_engine, Table, Column, Float, DateTime, String, MetaData, Integer
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

class CryptoDataCollector:
    def __init__(self, project_root: Optional[str] = None):
        """Initialize the CryptoDataCollector with database connection."""
        if project_root is None:
            # Assume you are in src/data and need to move up two levels
            self.project_root = Path(__file__).parent.parent.parent
        else:
            self.project_root = Path(project_root)

        # Define paths
        self.data_dir = self.project_root / 'data'
        self.db_path = self.project_root / 'crypto_data.db'

        # Create data directory if one does not exist
        self.data_dir.mkdir(exist_ok=True)

        if self.db_path.exists():
            try:
                self.db_path.unlink()
                print(f"Existing database removed: {self.db_path}")
            except Exception as e:
                print(f"Error removing database: {str(e)}")

        # Initialize database connection
        self.engine = create_engine(f"sqlite:///{self.db_path}")
        self.setup_logging()
        self.setup_database()


    def setup_logging(self):
        """Configure logging for the data collector."""
        if not logging.getLogger().hasHandlers():
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
        """Set up database tables."""
        metadata = MetaData()

        # Table for storing crypto price data
        self.prices_table = Table(
            'crypto_prices', 
            metadata,
            Column('timestamp', DateTime, primary_key=True),
            Column('crypto_id', String, primary_key=True),
            Column('price', Float),
            Column('market_cap', Float),
            Column('volume', Float),
            Column('percent_change_1h', Float),
            Column('percent_change_24h', Float),
            Column('percent_change_7d', Float),
            Column('percent_change_60d', Float),
            Column('percent_change_90d', Float)
        )
        
        # Table for storing crypto metadata
        self.metadata_table = Table(
            'crypto_metadata',
            metadata,
            Column('crypto_id', String, primary_key=True),
            Column('name', String),
            Column('symbol', String),
            Column('circulating_supply', Float),
            Column('total_supply', Float),
            Column('max_supply', Float),
            Column('num_market_pairs', Integer),
            Column('last_updated', DateTime)
        )

        metadata.create_all(self.engine)
        self.logger.info("Database tables created successfully")

    def process_csv_data(self, file_path: str) -> None:
        """
        Process cryptocurrency data from CSV file and store in database.
        """
        # Convert relative path to absolute using project root
        full_path = self.project_root / file_path

        try:
            self.logger.info(f"Processing CSV file: {full_path}")
            if not full_path.exists():
                raise FileNotFoundError(f"CSV file not found: {full_path}")
            
            # Read CSV file
            df = pd.read_csv(full_path)
            
            # Process timestamp
            current_time = datetime.now()
            
            # Prepare price data
            price_records = []
            metadata_records = []
            
            for _, row in df.iterrows():
                # Price data record
                price_record = {
                    'timestamp': current_time,
                    'crypto_id': row['Symbol'].lower(),  # Using symbol as crypto_id
                    'price': float(row['Price']),
                    'market_cap': float(row['Market Cap']),
                    'volume': float(row['Volume (24h)']),
                    'percent_change_1h': float(row['1h %']),
                    'percent_change_24h': float(row['24h %']),
                    'percent_change_7d': float(row['7d %']),
                    'percent_change_60d': float(row['60d %']),
                    'percent_change_90d': float(row['90d %'])
                }
                price_records.append(price_record)
                
                # Metadata record
                metadata_record = {
                    'crypto_id': row['Symbol'].lower(),
                    'name': row['Name'],
                    'symbol': row['Symbol'],
                    'circulating_supply': float(row['Circulating Supply']),
                    'total_supply': float(row['Total Supply']) if pd.notna(row['Total Supply']) else None,
                    'max_supply': float(row['Max Supply']) if pd.notna(row['Max Supply']) else None,
                    'num_market_pairs': int(row['Num Market Pairs']),
                    'last_updated': current_time
                }
                metadata_records.append(metadata_record)

            # Save to database
            self.save_to_database(price_records, metadata_records)
            self.logger.info(f"Successfully processed {len(price_records)} cryptocurrencies from CSV")

        except Exception as e:
            self.logger.error(f"Error processing CSV file: {str(e)}")
            raise

    def save_to_database(self, price_records: List[dict], metadata_records: List[dict]) -> None:
        """
        Save cryptocurrency data to database.
        
        Args:
            price_records: List of price data records
            metadata_records: List of metadata records
        """
        Session = sessionmaker(bind=self.engine)
        session = Session()

        try:
            # Save price data
            if price_records:
                session.execute(
                    self.prices_table.insert().prefix_with('OR REPLACE'),
                    price_records
                )

            # Save metadata
            if metadata_records:
                session.execute(
                    self.metadata_table.insert().prefix_with('OR REPLACE'),
                    metadata_records
                )

            session.commit()
            self.logger.info("Successfully saved data to database")

        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Database error: {str(e)}")
            raise
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
        
        query = f"SELECT * FROM crypto_prices"
        conditions = []

        if crypto_ids:
            crypto_list = ", ".join(f"'{id}'" for id in crypto_ids)
            conditions.append(f"crypto_id IN ({crypto_list})")

        if start_date:
            conditions.append(f"timestamp >= '{start_date}'")
        
        if end_date:
            conditions.append(f"timestamp <= '{end_date}'")  # Fixed: Changed >= to <=

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

if __name__ == "__main__":
    # Example usage
    collector = CryptoDataCollector()

    # Configure pandas display options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    
    # Process CSV file
    collector.process_csv_data('data/crypto_trends_insights_2024.csv')
    
    # Get data for all cryptocurrencies
    data = collector.get_crypto_data(
        start_date=datetime.now() - timedelta(days=1)
    )
    # print(data.head().to_string())

    # Sort by market cap and get top 10
    top_10_crypto = data.sort_values('market_cap', ascending=False).head(25)
    print("\nTop 10 Cryptocurrencies by Market Cap:")
    print(top_10_crypto.to_string())