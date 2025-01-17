from dataclasses import dataclass
from typing import Dict, Any
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@dataclass
class Config:
    mapping_file: str = os.path.join(os.path.dirname(__file__), "Mapping.csv")
    catalog_folder: str = os.path.join(os.path.dirname(__file__), "data")
    output_file: str = "database_globale.csv"
    openai_api_key: str = os.getenv('OPENAI_API_KEY', '')  # Get API key from environment variable
    
    @classmethod
    def from_user_input(cls) -> 'Config':
        # Default to 'data' folder in the current directory
        catalog_folder = os.path.join(os.path.dirname(__file__), "data")
        
        if not os.path.isdir(catalog_folder):
            raise ValueError(f"La cartella 'data' non esiste nel percorso: {catalog_folder}")
        if not os.path.isfile(cls.mapping_file):
            raise ValueError("Il file di mapping specificato non esiste.")
        if not cls.mapping_file.lower().endswith('.csv'):
            raise ValueError("Il file di mapping deve essere in formato CSV.")
            
        return cls()
