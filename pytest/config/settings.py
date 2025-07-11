from sqlalchemy import create_engine
# Paramètres de connexion

DATABASE_URL = 'postgresql://postgres:root@localhost:5432/mydb'
def test_engine():
    return create_engine(DATABASE_URL)