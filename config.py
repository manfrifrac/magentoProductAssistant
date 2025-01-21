from dataclasses import dataclass
from typing import Dict, Any
import os
from dotenv import load_dotenv
from pathlib import Path
import logging

# Load environment variables from .env file
load_dotenv()

class Config:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.data_folder = self.base_dir / "data"
        self.input_folder = self.data_folder / "input"
        self.output_folder = self.data_folder / "output"
        self.context_mapping_file = self.base_dir / "src/context/context_mapping.csv"
        self.mapping_file = self.base_dir / "src/Mapping.csv"  # Changed to use root Mapping.csv
        self.output_file = self.output_folder / "global_database.csv"
        
        # Enrichment related paths
        self.prompts_file = self.base_dir / "src/enrich/prompts.csv"
        self.global_database_file = self.output_folder / "global_database.csv"
        self.enriched_database_file = self.output_folder / "enriched_database.csv"
        
        # Create directory structure
        self._initialize_directories()
        
        # OpenAI configuration
        self.openai_api_key = "your-api-key-here"

    def _initialize_directories(self) -> None:
        """Create necessary directories and check for required files"""
        directories = [
            self.data_folder,
            self.input_folder,
            self.output_folder
        ]
        
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                logging.info(f"Directory ensured: {directory}")
            except Exception as e:
                logging.error(f"Failed to create directory {directory}: {e}")
                raise
                
        # Check for Excel files in supplier subdirectories
        excel_files = []
        supplier_dirs = [d for d in self.input_folder.glob("*") if d.is_dir()]
        
        for supplier_dir in supplier_dirs:
            excel_files.extend(list(supplier_dir.glob("*.xlsx")))
            
        if not excel_files:
            logging.warning(f"No Excel files found in supplier directories under: {self.input_folder}")
        else:
            logging.info(f"Found {len(excel_files)} Excel files in {len(supplier_dirs)} supplier directories")
            for supplier_dir in supplier_dirs:
                files = list(supplier_dir.glob("*.xlsx"))
                if files:
                    logging.info(f"  {supplier_dir.name}: {len(files)} files")
            
        # Check if mapping files exist
        required_files = [
            (self.mapping_file, "Magento mapping"),
            (self.context_mapping_file, "Context mapping")
        ]
        
        for file_path, file_desc in required_files:
            if not file_path.exists():
                logging.error(f"Required {file_desc} file not found: {file_path}")
                raise FileNotFoundError(f"Missing required file: {file_path}")
            else:
                logging.info(f"Found {file_desc} file: {file_path}")

    def _load_openai_key(self) -> str:
        """Load OpenAI API key from environment"""
        key = os.getenv('OPENAI_API_KEY')
        if not key:
            logging.warning("OpenAI API key not found in environment variables")
        return key
