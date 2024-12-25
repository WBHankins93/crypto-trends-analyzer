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
