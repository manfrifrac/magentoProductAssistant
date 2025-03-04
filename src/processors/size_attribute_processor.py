import re
from typing import Dict, Optional
import pandas as pd
import logging
import json
from pathlib import Path

class SizeAttributeProcessor:
    def __init__(self, config_path: Optional[Path] = None):
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / 'config' / 'supplier_size_mapping.json'
            
        with open(config_path) as f:
            self.config = json.load(f)
        
        self.size_patterns = {
            'abbigliamento': [
                r'^(XS|S|M|L|XL|XXL|2XL|3XL)$',  # Exact matches
                r'^(S|M|L|XL)-[SML]$',  # Ranges like S-M, M-L
                r'^(S|M|L|XL)/[SML]$',  # Ranges like S/M, M/L
                r'^(UNICA|TU|U|UNIVERSAL|ONE SIZE)$'  # Universal sizes
            ],
            'bambino': [
                r'(\d+)\s*CM\s*/\s*(\d+)-(\d+)\s*YEARS?',  # 110 CM / 3-4 YEARS
                r'(\d+)\s*CM\s*/\s*(\d+)\s*YEARS?',  # 110 CM / 3 YEARS
                r'(\d+)-(\d+)\s*YEARS?',  # 3-4 YEARS
                r'(\d+)/(\d+)\s*YEARS?',  # 3/4 YEARS
                r'(\d+)\s*Y(EARS)?',  # 3Y or 3YEARS
                r'^(\d{2,3})\s*CM$',  # 110 CM
                r'^(\d{1,2})$'  # Single digits for kids sizes (e.g., 5, 8)
            ],
            'calzature': [
                r'^(\d{2})[-/](\d{2})$',  # 36-37 or 36/37
                r'^(\d{2})$'  # 36
            ],
            'cappelli': [
                r'^(54|56|58|60)$',  # Hat sizes
                r'^(54|56|58|60)\s*CM$'
            ]
        }

    def process_size(self, product_data: Dict, supplier: str, original_row: Dict = None) -> Dict[str, str]:
        """Process size information for a product"""
        size_value = product_data.get('size', '')
        
        if not size_value or pd.isna(size_value):
            return {
                'size': '',
                'size_set': '',
                'size_type': ''
            }

        # Clean and standardize the size value
        size_value = str(size_value).strip().upper()
        
        if supplier.lower() == 'espa':
            # ESPA specific size processing
            if re.match(r'^[XS|S|M|L|XL|2XL|3XL]$', size_value):
                return {
                    'size': size_value,
                    'size_set': 'abbigliamento',
                    'size_type': 'clothing'
                }
            elif re.match(r'^\d{2}$', size_value):  # Numeric sizes like 42, 44, etc.
                size_num = int(size_value)
                if 32 <= size_num <= 54:  # Common clothing sizes
                    return {
                        'size': size_value,
                        'size_set': 'abbigliamento',
                        'size_type': 'clothing'
                    }
                elif 2 <= size_num <= 16:  # Kids sizes
                    return {
                        'size': size_value,
                        'size_set': 'bambino',
                        'size_type': 'kids'
                    }
            
        # First check if it's a single digit number (likely kids size)
        if re.match(r'^\d{1,2}$', size_value):
            numeric_size = int(size_value)
            if 2 <= numeric_size <= 16:  # Common kids size range
                return {
                    'size': size_value,
                    'size_set': 'bambino',
                    'size_type': 'kids'
                }

        # First try exact matches from supplier config
        supplier_config = self.config.get('size_sets', {}).get(supplier.lower(), {})
        for set_name, valid_sizes in supplier_config.items():
            if any(s.upper() == size_value for s in valid_sizes):
                return {
                    'size': size_value,
                    'size_set': set_name,
                    'size_type': self.config['size_type_mapping'].get(set_name, 'clothing')
                }
        
        # Then try pattern matching
        for size_set, patterns in self.size_patterns.items():
            for pattern in patterns:
                if re.search(pattern, size_value):
                    return {
                        'size': size_value,
                        'size_set': size_set,
                        'size_type': self.config['size_type_mapping'].get(size_set, 'clothing')
                    }
        
        # Try category indicators as fallback
        for category, indicators in self.config['category_indicators'].items():
            if any(indicator.upper() in size_value for indicator in indicators):
                return {
                    'size': size_value,
                    'size_set': category,
                    'size_type': self.config['size_type_mapping'].get(category, 'clothing')
                }

        # Check specific patterns if not matched yet
        if re.search(r'\d+\s*CM.*YEARS?', size_value):  # e.g. "110 CM / 3-4 YEARS"
            return {'size': size_value, 'size_set': 'bambino', 'size_type': 'kids'}
            
        if re.search(r'^(\d{2})[-/]?(\d{2})?$', size_value):  # e.g. "36" or "36-37"
            numbers = [int(n) for n in re.findall(r'\d{2}', size_value)]
            if all(35 <= n <= 46 for n in numbers):
                return {'size': size_value, 'size_set': 'calzature', 'size_type': 'shoes'}
                
        if any(x in size_value for x in ['UNICA', 'TU', 'U', 'UNIVERSAL', 'ONE SIZE']):
            return {'size': size_value, 'size_set': 'abbigliamento', 'size_type': 'clothing'}

        # Use size patterns for final attempt
        if any(s in size_value for s in ['S', 'M', 'L', 'XL', '2XL', '3XL']):
            return {'size': size_value, 'size_set': 'abbigliamento', 'size_type': 'clothing'}
            
        if any(s in size_value for s in ['54', '56', '58', '60']) and len(size_value) <= 4:
            return {'size': size_value, 'size_set': 'cappelli', 'size_type': 'accessories'}
            
        # Only use abbigliamento as last resort if nothing else matches
        logging.warning(f"Could not determine specific size set for value: {size_value}. Using fallback.")
        return {
            'size': size_value,
            'size_set': 'abbigliamento',
            'size_type': 'clothing'
        }

    def detect_category(self, product_data: Dict, size_value: str) -> str:
        """Simplified category detection based on size value"""
        if not size_value:
            return 'default'
            
        size_value = str(size_value).upper()
        
        if any(x in size_value for x in ['CM', 'YEARS', 'Y', 'CHILD']):
            return 'bambini'
        elif 'ONE SIZE' in size_value:
            return 'onesize'
        elif any(x in size_value for x in ['S', 'M', 'L', 'XL']):
            return 'abbigliamento'
            
        return 'default'

    def extract_size(self, sku, description, supplier):
        """Extract size from ESPA product code format"""
        if supplier.lower() != 'espa':
            return None
            
        # Check size-color column first
        size_color = description.split('-')[0].strip() if description else None
        
        if size_color:
            # Extract numeric size pattern (e.g., "42" from "42-Black")
            size_match = re.match(r'^(\d{2,3})', size_color)
            if size_match:
                return size_match.group(1)
            
            # Extract letter size pattern (e.g., "XL" from "XL-Red") 
            letter_match = re.match(r'^(XS|S|M|L|XL|XXL|2XL|3XL)', size_color)
            if letter_match:
                return letter_match.group(1)
                
        return None

    def get_size_set_and_type(self, description):
        if not description:
            return 'default', 'default'
            
        desc_lower = description.lower()
        
        # Check category indicators
        for category, indicators in self.size_config['category_indicators'].items():
            for indicator in indicators:
                if indicator.lower() in desc_lower:
                    return (category, 
                           self.size_config['size_type_mapping'].get(category, 'default'))
                           
        return 'default', 'default'
