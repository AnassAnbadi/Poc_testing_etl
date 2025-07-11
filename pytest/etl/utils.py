import pandas as pd
from datetime import datetime


def calculate_age(birth_date: pd.Timestamp, snapshot_date: pd.Timestamp) -> int:
    """Calcule l'âge à partir de la date de naissance et de la date snapshot."""
    if pd.isnull(birth_date):
        return None
    age = snapshot_date.year - birth_date.year
    if (snapshot_date.month, snapshot_date.day) < (birth_date.month, birth_date.day):
        age -= 1
    return age

