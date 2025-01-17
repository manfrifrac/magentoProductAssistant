import pandas as pd
from typing import Dict, List, Optional, Any
import os
import logging
from config import Config

class CatalogProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.supplier_mappings: Dict[str, Dict[str, List[str]]] = {}
        self.magento_fields: List[str] = []
        self.default_values: Dict[str, Any] = {}

    def load_mapping(self) -> None:
        """Load and parse the mapping file."""
        try:
            df = pd.read_csv(self.config.mapping_file, encoding='latin1')
            
            # Store Magento fields and defaults
            self.magento_fields = df["Magento Field"].dropna().unique().tolist()
            self.default_values = df.set_index("Magento Field")["Default"].to_dict()
            
            # Process supplier mappings
            for supplier in df.columns[2:]:  # Skip Magento Field and Default columns
                supplier_key = supplier.strip().lower()
                self.supplier_mappings[supplier_key] = {}
                
                for _, row in df.iterrows():
                    magento_field = row["Magento Field"]
                    supplier_col = row[supplier]
                    
                    if pd.notna(magento_field) and pd.notna(supplier_col):
                        if magento_field not in self.supplier_mappings[supplier_key]:
                            self.supplier_mappings[supplier_key][magento_field] = []
                        self.supplier_mappings[supplier_key][magento_field].append(supplier_col)
                        
            logging.debug(f"Loaded mappings for suppliers: {list(self.supplier_mappings.keys())}")
            
        except Exception as e:
            logging.error(f"Error loading mapping file: {e}")
            raise

    def extract_filename(self, url: str) -> str:
        """Extract just the filename from an image URL or path."""
        if not url or pd.isna(url):
            return ''
        url = str(url).strip()
        # Return empty if no valid content
        if url.lower() == 'nan' or not url:
            return ''
        # Extract filename from URL/path
        filename = os.path.basename(url)
        # Ensure .jpg extension
        if not filename.lower().endswith('.jpg'):
            filename = f"{filename.split('.')[0]}.jpg"
        return filename

    def process_catalog(self, supplier_name: str, file_path: str) -> Optional[pd.DataFrame]:
        """Process a single catalog file."""
        try:
            logging.info(f"Reading Excel file for {supplier_name}...")
            df = pd.read_excel(file_path)
            logging.info(f"Excel file read successfully. Processing {len(df)} rows for {supplier_name}")
            
            supplier_key = supplier_name.strip().lower()
            mapping = self.supplier_mappings.get(supplier_key, {})
            
            if not mapping:
                logging.warning(f"No mapping found for supplier: {supplier_name}")
                return None
                
            processed = pd.DataFrame(index=df.index)
            logging.info(f"Processing {len(self.magento_fields)} Magento fields for {supplier_name}")
            
            # Process each Magento field
            for magento_field in self.magento_fields:
                logging.debug(f"Processing field '{magento_field}' for {supplier_name}")
                if magento_field in mapping:
                    supplier_cols = mapping[magento_field]
                    existing_cols = [col for col in supplier_cols if col in df.columns]
                    
                    if existing_cols:
                        if magento_field in ['base_image', 'small_image', 'thumbnail_image']:
                            # Process single image fields
                            img_url = str(df[existing_cols[0]].iloc[0])
                            processed[magento_field] = self.extract_filename(img_url)
                            
                        elif magento_field == 'additional_images':
                            logging.info(f"Processing additional images from {len(existing_cols)} columns for {supplier_name}")
                            
                            def combine_images(row):
                                valid_images = []
                                for col in existing_cols:
                                    filename = self.extract_filename(row[col])
                                    if filename:
                                        valid_images.append(filename)
                                return ','.join(valid_images) if valid_images else ''

                            processed[magento_field] = df[existing_cols].apply(combine_images, axis=1)
                            
                            # Log statistics about found images
                            rows_with_images = processed[magento_field].str.len() > 0
                            num_rows_with_images = rows_with_images.sum()
                            
                            if num_rows_with_images > 0:
                                avg_images = processed[magento_field][rows_with_images].str.count(',').mean() + 1
                                logging.info(f"Found images in {num_rows_with_images} products, average {avg_images:.1f} images per product")
                            else:
                                logging.warning(f"No additional images found for {supplier_name}")
                            
                        else:
                            if len(existing_cols) == 1:
                                processed[magento_field] = df[existing_cols[0]]
                            else:
                                # Combine multiple columns, excluding empty values
                                processed[magento_field] = df[existing_cols].fillna('').astype(str).apply(
                                    lambda x: ','.join(filter(None, x.str.strip()))
                                )
                    else:
                        processed[magento_field] = self.default_values.get(magento_field, '')
                else:
                    processed[magento_field] = self.default_values.get(magento_field, '')
            
            logging.info(f"Completed processing for {supplier_name}")
            processed['fornitore'] = supplier_name
            
            return processed
            
        except Exception as e:
            logging.error(f"Error processing catalog {file_path}: {e}")
            return None

    def process_all_catalogs(self) -> None:
        logging.info("Starting catalog processing...")
        all_data = []
        stats = {'suppliers': 0, 'files': 0, 'rows': 0}
        
        suppliers = os.listdir(self.config.catalog_folder)
        logging.info(f"Found {len(suppliers)} supplier directories to process")
        
        for supplier_name in suppliers:
            logging.info(f"Processing supplier directory: {supplier_name}")
            supplier_path = os.path.join(self.config.catalog_folder, supplier_name)
            
            if not os.path.isdir(supplier_path):
                continue
                
            excel_files = [f for f in os.listdir(supplier_path) 
                         if f.endswith((".xlsx", ".xls"))]
            
            if not excel_files:
                logging.warning(f"No Excel files found for supplier: {supplier_name}")
                continue
                
            file_path = os.path.join(supplier_path, excel_files[0])
            result = self.process_catalog(supplier_name, file_path)
            
            if result is not None:
                all_data.append(result)
                stats['suppliers'] += 1
                stats['files'] += 1
                stats['rows'] += len(result)
                logging.info(f"Processed {supplier_name}: {len(result)} rows")
        
        if all_data:
            logging.info(f"Combining data from {len(all_data)} suppliers...")
            final_df = pd.concat(all_data, ignore_index=True)
            
            logging.info("Removing duplicate entries...")
            initial_rows = len(final_df)
            final_df = final_df.drop_duplicates(subset=['sku', 'EAN'], keep='first')
            duplicates_removed = initial_rows - len(final_df)
            
            logging.info(f"Saving output file ({len(final_df)} rows, {duplicates_removed} duplicates removed)...")
            final_df.to_csv(self.config.output_file, index=False)
            
            logging.info(f"Generated database: {self.config.output_file}")
            logging.info(f"Total suppliers: {stats['suppliers']}")
            logging.info(f"Total files: {stats['files']}")
            logging.info(f"Total rows: {stats['rows']}")
        else:
            logging.warning("No data processed")
