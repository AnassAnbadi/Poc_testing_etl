from etl.transform import transform_data
from etl.extract import extract_data
import pandas as pd
import pytest
from sqlalchemy import create_engine
from config.settings import DATABASE_URL

@pytest.fixture
def test_engine():
    return create_engine(DATABASE_URL)

@pytest.fixture
def test_data(test_engine):
    """Fixture to provide test data for all transformation tests"""
    source_df, target_df = extract_data(test_engine)
    return source_df, target_df

@pytest.fixture
def transformed_data(test_data):
    """Fixture to provide transformed data for validation tests"""
    source_df, target_df = test_data
    return transform_data(source_df, target_df)

def test_extraction_returns_non_empty_dataframes(test_data):
    """Test that extraction returns non-empty dataframes"""
    source_df, target_df = test_data
    assert not source_df.empty, "Source dataframe is empty"
    assert not target_df.empty, "Target dataframe is empty"

def test_transform_returns_dataframe(test_data):
    """Test that transform function returns a pandas DataFrame"""
    source_df, target_df = test_data
    transformed_df = transform_data(source_df, target_df)
    assert isinstance(transformed_df, pd.DataFrame)

def test_transformed_data_structure(transformed_data):
    """Test the structure of the transformed dataframe"""
    expected_columns = ["snapshot_date", "id", "datenaissance", "age"]
    assert list(transformed_data.columns) == expected_columns

def test_transformed_data_column_types(transformed_data):
    """Test the data types of columns in transformed dataframe"""
    assert pd.api.types.is_datetime64_any_dtype(transformed_data["snapshot_date"])
    assert pd.api.types.is_datetime64_any_dtype(transformed_data["datenaissance"])
    assert pd.api.types.is_integer_dtype(transformed_data["id"])
    assert pd.api.types.is_integer_dtype(transformed_data["age"])


def test_transformed_data_content_constraints(transformed_data):
    """Test content constraints of the transformed data"""
    # Check for null values with detailed error reporting
    def check_null(column, name):
        null_count = transformed_data[column].isnull().sum()
        assert null_count == 0, (
            f"Found {null_count} null values in {name}\n"
            f"Sample of problematic rows:\n"
            f"{transformed_data[transformed_data[column].isnull()].head(3).to_string()}"
        )
    
    check_null("id", "ID column")
    check_null("snapshot_date", "snapshot_date column")
    check_null("datenaissance", "birth date column")
    check_null("age", "age column")
    
    # Check age is non-negative with detailed reporting
    negative_ages = transformed_data[transformed_data["age"] < 0]
    assert len(negative_ages) == 0, (
        f"Found {len(negative_ages)} rows with negative age\n"
        f"Sample of problematic rows:\n"
        f"{negative_ages.head(3).to_string()}"
    )
    
    # Additional validation - check age is reasonable (0-120)
    invalid_ages = transformed_data[(transformed_data["age"] > 120) | (transformed_data["age"] < 0)]
    assert len(invalid_ages) == 0, (
        f"Found {len(invalid_ages)} rows with unrealistic age (not between 0-120)\n"
        f"Sample of problematic rows:\n"
        f"{invalid_ages.head(3).to_string()}"
    )
    
    # Verify date consistency (age matches birth date)
    if 'snapshot_date' in transformed_data.columns and 'datenaissance' in transformed_data.columns:
        calculated_age = (
            (transformed_data["snapshot_date"] - transformed_data["datenaissance"])
            .dt.days / 365.25
        ).astype(int)
        
        age_mismatch = transformed_data[abs(calculated_age - transformed_data["age"]) > 1]
        assert len(age_mismatch) == 0, (
            f"Found {len(age_mismatch)} rows where age doesn't match birth date\n"
            f"Sample of problematic rows:\n"
            f"{age_mismatch[['snapshot_date', 'datenaissance', 'age']].head(3).to_string()}"
        )

def test_transformation_logging(test_data, caplog):
    """Test that transformation logs appropriate messages"""
    caplog.set_level("INFO")
    source_df, target_df = test_data
    _ = transform_data(source_df, target_df)
    
    # Check log messages
    assert "Début de la transformation des données..." in caplog.text
    assert "Fusion réalisée" in caplog.text
    assert "Calcul des âges terminé avec succès." in caplog.text