from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Paramètres de connexion

load_dotenv()


ALERT_EMAIL = os.getenv("ALERT_EMAIL")
ALERT_PASSWORD = os.getenv("ALERT_PASSWORD")


DATABASE_URL = 'postgresql://postgres:root@localhost:5432/postgres'
def test_engine():
    return create_engine(DATABASE_URL)