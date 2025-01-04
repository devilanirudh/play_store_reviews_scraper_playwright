
import psycopg2
import os

from dotenv import load_dotenv
import os
from pathlib import Path

# Get the base directory (where .env should be)
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env file
load_dotenv(BASE_DIR / '.env')

class Settings:
    def __init__(self):
        self.DATABASE_URL = os.environ.get('DB_URL')
        if not self.DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable is not set")
        print(f"Raw DATABASE_URL: {self.DATABASE_URL}")  # Debug print

settings = Settings()