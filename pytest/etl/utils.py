import pandas as pd
from datetime import datetime


def calculate_age(birth_date, reference_date):
    age = (reference_date - birth_date).days / 365.2425
    # print("Calculated age:", age)
    if pd.isnull(age) or age < 0:
        return 0
    return int(age)
