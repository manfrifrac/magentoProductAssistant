import pandas as pd
from typing import Union, Dict, List
import os
import logging
from config import Config
from src.context.product_context import ProductContext

class CatalogProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.context_mapping_file = config.context_mapping_file
        self.magento_mapping_file = config.mapping_file
        self.field_mappings = []
        self.magento_mappings: Dict[str, Dict[str, List[str]]] = {}
        self.required_fields: List[str] = []
        self.default_values: Dict[str, str] = {}
        self.image_fields = ['base_image', 'small_image', 'thumbnail_image', 'additional_images']

    def _strip_image_url(self, image_path: str) -> str:
        """Strip URL from image path to get just the filename"""
        if pd.isna(image_path) or not image_path:
            return ''
        # Split on both forward and back slashes and take the last part
        return image_path.replace('\\', '/').split('/')[-1]

    def load_mapping(self) -> None:
        """Load both context and Magento field mappings"""
        try:
            # Load context mappings
            context_df = pd.read_csv(self.context_mapping_file)
            self.field_mappings = context_df.to_dict('records')
            logging.info(f"Loaded {len(self.field_mappings)} context mappings")
            
            # Load Magento mappings
            magento_df = pd.read_csv(self.magento_mapping_file)
            
            # Get required fields and default values
            self.required_fields = magento_df[magento_df['magento_field'].isin([
                'sku', 'EAN', 'base_image', 'small_image', 'thumbnail_image', 'price'
            ])]['magento_field'].tolist()
            
            self.default_values = {
                row['magento_field']: row['default']
                for _, row in magento_df.iterrows()
                if pd.notna(row['default'])
            }
            
            # Process supplier mappings
            suppliers = [col for col in magento_df.columns if col.lower() in ['guirca', 'widmann']]
            
            for supplier in suppliers:
                supplier_key = supplier.lower()
                self.magento_mappings[supplier_key] = {}
                
                for _, row in magento_df.iterrows():
                    magento_field = row['magento_field']
                    supplier_field = row[supplier]
                    
                    if pd.notna(supplier_field):
                        if magento_field not in self.magento_mappings[supplier_key]:
                            self.magento_mappings[supplier_key][magento_field] = []
                        self.magento_mappings[supplier_key][magento_field].append(supplier_field)
            
            logging.info(f"Loaded Magento mappings for suppliers: {list(self.magento_mappings.keys())}")
            
        except Exception as e:
            logging.error(f"Error loading mappings: {e}")
            raise

    def process_catalog(self, supplier_name: str, file_path: str) -> Union[pd.DataFrame, None]:
        try:
            df = pd.read_excel(file_path)
            supplier_key = supplier_name.lower()
            
            if supplier_key not in self.magento_mappings:
                logging.error(f"No Magento mapping found for supplier {supplier_name}")
                return None
                
            mapping = self.magento_mappings[supplier_key]
            result_data = []
            
            for idx, row in df.iterrows():
                try:
                    magento_row = {}
                    
                    # Process each Magento field
                    for magento_field, supplier_fields in mapping.items():
                        if magento_field == 'additional_images':
                            # Combine all image fields and strip URLs
                            images = []
                            for field in supplier_fields:
                                if field in row and pd.notna(row[field]):
                                    images.append(self._strip_image_url(str(row[field])))
                            magento_row[magento_field] = ','.join(images)
                        elif magento_field in self.image_fields:
                            # Strip URLs from single image fields
                            for field in supplier_fields:
                                if field in row and pd.notna(row[field]):
                                    magento_row[magento_field] = self._strip_image_url(str(row[field]))
                                    break
                        else:
                            # Handle non-image fields as before
                            for field in supplier_fields:
                                if field in row and pd.notna(row[field]):
                                    magento_row[magento_field] = row[field]
                                    break
                            
                    # Add default values for missing required fields
                    for field in self.required_fields:
                        if field not in magento_row or pd.isna(magento_row[field]):
                            magento_row[field] = self.default_values.get(field, '')
                    
                    # Add supplier name
                    magento_row['fornitore'] = supplier_name
                    
                    # Add context data
                    context = ProductContext(dict(row), supplier_name, self.context_mapping_file)
                    magento_row['product_context'] = context.get_context()
                    
                    result_data.append(magento_row)
                    
                    if idx % 1000 == 0:
                        logging.info(f"Processed {idx + 1}/{len(df)} products")
                        
                except Exception as e:
                    logging.error(f"Error processing row {idx}: {str(e)}")
                    continue
                    
            return pd.DataFrame(result_data)
            
        except Exception as e:
            logging.error(f"Error processing catalog: {e}")
            return None

    def process_all_catalogs(self) -> None:
        """Process all catalogs and create a single Magento-compatible output file"""
        try:
            input_dir = self.config.input_folder
            all_data = []
            
            supplier_dirs = [d for d in input_dir.glob("*") if d.is_dir()]
            
            if not supplier_dirs:
                logging.warning(f"No supplier directories found in {input_dir}")
                return
                
            for supplier_dir in supplier_dirs:
                supplier_name = supplier_dir.name
                excel_files = list(supplier_dir.glob("*.xlsx"))
                
                if not excel_files:
                    continue
                    
                for file_path in excel_files:
                    result_df = self.process_catalog(supplier_name, str(file_path))
                    if result_df is not None:
                        all_data.append(result_df)
            
            if all_data:
                # Combine all data
                final_df = pd.concat(all_data, ignore_index=True)
                
                # Remove duplicates based on SKU
                final_df = final_df.drop_duplicates(subset=['sku'], keep='first')
                
                # Save to single output file
                final_df.to_csv(self.config.output_file, index=False)
                logging.info(f"Saved combined catalog to {self.config.output_file}")
                
        except Exception as e:
            logging.error(f"Error in process_all_catalogs: {e}")
            raise
