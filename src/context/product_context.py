import logging
import pandas as pd
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

@dataclass
class FieldMapping:
    supplier_field: str
    context_type: str

class ProductContext:
    def __init__(self, product_data: dict, supplier: str, mapping_file: str):
        self.product_data = self._normalize_data(product_data)
        self.supplier = supplier
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.ERROR)  # Set logger level to ERROR
        self.load_mapping(mapping_file)

    def _normalize_data(self, data: dict) -> dict:
        """Normalize dictionary keys to uppercase for matching"""
        return {str(k).upper(): v for k, v in data.items()}

    def load_mapping(self, mapping_file: str) -> None:
        try:
            self.mapping_df = pd.read_csv(mapping_file)
            mappings = self.mapping_df[
                self.mapping_df['supplier'] == self.supplier
            ][['supplier_field', 'context_type']].values.tolist()
            
            self.field_mappings = [
                FieldMapping(str(sf).upper(), ct) 
                for sf, ct in mappings
            ]
            
            if not self.field_mappings:
                raise ValueError(f"No mappings found for supplier {self.supplier}")
                
        except Exception as e:
            self.logger.error(f"Error loading mappings for {self.supplier}: {e}")
            raise

    def _clean_value(self, value: Any) -> Optional[str]:
        if pd.isna(value) or value is None:
            return None
        try:
            if isinstance(value, (int, float)):
                return str(int(value)) if float(value).is_integer() else str(float(value))
            
            str_value = str(value).strip()
            return str_value if str_value and str_value.lower() != 'nan' else None
        except Exception as e:
            self.logger.error(f"Error cleaning value {value}: {e}")
            return None

    def get_context(self) -> str:
        """Returns all context fields as a single formatted string with key:value pairs"""
        try:
            context_pairs = []
            
            for mapping in self.field_mappings:
                supplier_field = mapping.supplier_field
                if supplier_field in self.product_data:
                    value = self._clean_value(self.product_data[supplier_field])
                    if value:
                        context_pairs.append(f"{mapping.context_type}:{value}")

            return " | ".join(context_pairs) if context_pairs else ""

        except Exception as e:
            self.logger.error(f"Error generating context: {e}")
            return ""

