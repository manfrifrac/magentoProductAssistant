import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Union

def safe_read_csv(file_path: Union[str, Path], **kwargs) -> Optional[pd.DataFrame]:
    """Safely read a CSV file with proper error handling"""
    try:
        return pd.read_csv(file_path, **kwargs)
    except Exception as e:
        logging.error(f"Error reading CSV file {file_path}: {e}")
        return None

def safe_read_excel(file_path: Union[str, Path], **kwargs) -> Optional[pd.DataFrame]:
    """Safely read an Excel file with proper error handling"""
    try:
        return pd.read_excel(file_path, **kwargs)
    except Exception as e:
        logging.error(f"Error reading Excel file {file_path}: {e}")
        return None

def ensure_columns(df: pd.DataFrame, required_columns: list) -> pd.DataFrame:
    """Ensure DataFrame has required columns, initializing empty ones if needed"""
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''
    return df
