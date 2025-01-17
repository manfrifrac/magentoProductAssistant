import pandas as pd
from typing import Dict, List, Optional, Any, Set
import os
import logging
from config import Config
import re
from collections import defaultdict

class CatalogProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.supplier_mappings: Dict[str, Dict[str, List[str]]] = {}
        self.magento_fields: List[str] = []
        self.default_values: Dict[str, Any] = {}
        self._descriptive_fields_cache: Dict[str, Dict[str, bool]] = {}  # Cache per fornitore
        self.field_categories = {
            'dimensions': r'(?i)(dimension|size|talla|misur|altezza|larghezza|prof|taglia)',
            'colors': r'(?i)(colou?r|colore?s)',
            'materials': r'(?i)(material|compos|fabric|tessut)',
            'categories': r'(?i)(categ|tipo|class|group)',
            'descriptions': r'(?i)(desc|detail|spec|caratt)',
            'themes': r'(?i)(theme|tema|occas|event)',
            'features': r'(?i)(feat|carat|funz|options)',
            'brand': r'(?i)(brand|marca|produt|manufac)',
            'target': r'(?i)(target|età|age|gender|sex)',
            'packaging': r'(?i)(pack|conf|imballo|content)',
            'season': r'(?i)(season|stag|temp)',
            'technical': r'(?i)(tech|specs|specific)'
        }

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

    def analyze_field_content(self, values: pd.Series) -> Dict[str, float]:
        """Analizza il contenuto di un campo e restituisce le metriche."""
        non_empty = values.dropna().astype(str)
        if len(non_empty) == 0:
            return {"score": 0, "is_descriptive": False}

        metrics = {
            "avg_length": non_empty.str.len().mean(),
            "avg_words": non_empty.str.split().str.len().mean(),
            "unique_ratio": len(non_empty.unique()) / len(non_empty),
            "numeric_ratio": non_empty.str.contains(r'^\d+$').mean(),
            "special_chars_ratio": non_empty.str.contains(r'[^a-zA-Z0-9\s]').mean()
        }
        
        # Calcola uno score complessivo
        score = (
            (metrics["avg_length"] > 15) * 2 +
            (metrics["avg_words"] > 3) * 2 +
            (metrics["unique_ratio"] > 0.3) * 1.5 +
            (metrics["numeric_ratio"] < 0.5) * 1 +
            (metrics["special_chars_ratio"] < 0.3) * 0.5
        )
        
        return {
            **metrics,
            "score": score,
            "is_descriptive": score > 3  # Soglia per considerare un campo descrittivo
        }

    def get_descriptive_fields(self, df: pd.DataFrame, mapped_columns: Set[str]) -> Dict[str, bool]:
        """Identifica i campi descrittivi nel DataFrame."""
        descriptive_fields = {}
        
        for col in df.columns:
            if col not in mapped_columns and df[col].dtype == object:
                analysis = self.analyze_field_content(df[col])
                descriptive_fields[col] = analysis["is_descriptive"]
                
                if analysis["is_descriptive"]:
                    logging.debug(f"Campo descrittivo trovato: {col} (score: {analysis['score']:.2f})")
                    
        return descriptive_fields

    def create_context(self, row: pd.Series, descriptive_fields: Dict[str, bool]) -> Dict[str, str]:
        """Crea il contesto per una riga usando solo i campi descrittivi."""
        context = {}
        
        for col, is_descriptive in descriptive_fields.items():
            if is_descriptive and not pd.isna(row[col]):
                value = str(row[col]).strip()
                if value and len(value) > 10:
                    context[col.lower()] = value
                    
        return context if context else None

    def is_descriptive_field(self, field_values: pd.Series) -> bool:
        """Determine if a field contains descriptive content based on its values."""
        if field_values.dtype == object:  # solo campi testuali
            # Calcola statistiche sul campo
            non_empty_values = field_values.dropna().astype(str)
            if len(non_empty_values) == 0:
                return False
                
            avg_length = non_empty_values.str.len().mean()
            word_counts = non_empty_values.str.split().str.len()
            avg_words = word_counts.mean() if not word_counts.empty else 0
            
            # Un campo è considerato descrittivo se:
            # - Ha in media più di X caratteri
            # - Ha in media più di Y parole
            # - Non contiene valori ripetuti frequentemente (varietà)
            unique_ratio = len(non_empty_values.unique()) / len(non_empty_values)
            
            return (avg_length > 15 and  # lunghezza media minima
                    avg_words > 3 and    # numero medio di parole
                    unique_ratio > 0.3)   # varietà dei valori
        return False

    def categorize_field(self, field_name: str) -> str:
        """Categorizza un campo in base al suo nome."""
        field_lower = field_name.lower()
        for category, pattern in self.field_categories.items():
            if re.search(pattern, field_lower):
                return category
        return 'other'

    def is_valuable_content(self, value: Any) -> bool:
        """Verifica se un valore contiene informazioni utili."""
        if pd.isna(value):
            return False
        str_value = str(value).strip()
        if not str_value or str_value.lower() == 'nan':
            return False
        # Esclude valori troppo corti o solo numerici
        if len(str_value) < 2 or str_value.isdigit():
            return False
        return True

    def create_universal_context(self, row: pd.Series, df_columns: List[str], mapped_columns: Set[str]) -> Dict[str, Any]:
        """Crea un contesto universale per il prodotto."""
        context = defaultdict(list)
        
        for col in df_columns:
            if col not in mapped_columns and self.is_valuable_content(row[col]):
                value = str(row[col]).strip()
                category = self.categorize_field(col)
                context[category].append({
                    'field': col,
                    'value': value
                })
        
        # Organizza il contesto finale
        final_context = {}
        for category, items in context.items():
            if items:  # Solo categorie non vuote
                if len(items) == 1:
                    final_context[category] = items[0]['value']
                else:
                    # Combina valori multipli della stessa categoria
                    final_context[category] = [item['value'] for item in items]
        
        return final_context if final_context else None

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

            # Create set of all mapped supplier columns
            mapped_columns = set()
            for magento_field, supplier_cols in mapping.items():
                mapped_columns.update(supplier_cols)

            # Identifica i campi descrittivi una volta sola per fornitore
            if supplier_key not in self._descriptive_fields_cache:
                self._descriptive_fields_cache[supplier_key] = self.get_descriptive_fields(df, mapped_columns)
            
            descriptive_fields = self._descriptive_fields_cache[supplier_key]
            
            # Create universal context
            processed['additional_context'] = df.apply(
                lambda row: self.create_universal_context(row, df.columns, mapped_columns),
                axis=1
            )

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
