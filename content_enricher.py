import pandas as pd
import openai
import logging
import time
from typing import Dict, Optional
from config import Config

class ContentEnricher:
    def __init__(self, config: Config):
        self.config = config
        self.prompts: Dict[str, str] = {}
        self.openai_client = openai.OpenAI(api_key=config.openai_api_key)
        self.fields_to_enrich = ['name', 'description', 'short_description', 'url_key']
        
    def load_prompts(self):
        """Load prompt templates from mapping file."""
        try:
            # Read only needed columns and handle variable number of columns
            df = pd.read_csv(
                self.config.mapping_file, 
                usecols=['Magento Field', 'Prompt Template'],
                skipinitialspace=True,
                sep=',',
                on_bad_lines='skip'  # Skip problematic lines
            )
            
            # Clean and filter prompts
            prompts_series = df.dropna(subset=['Prompt Template'])
            self.prompts = prompts_series.set_index('Magento Field')['Prompt Template'].to_dict()
            
            logging.info(f"Loaded {len(self.prompts)} prompt templates")
            logging.debug(f"Available prompts for fields: {list(self.prompts.keys())}")
            
        except Exception as e:
            logging.error(f"Error loading prompts: {e}")
            raise

    def generate_content(self, field: str, row: pd.Series) -> Optional[str]:
        """Generate content for a specific field using OpenAI."""
        if field not in self.prompts:
            return None
            
        try:
            # Get category list as comma-separated string
            categories = str(row.get('categories', ''))
            
            # Prepare context data
            context = {
                'name': str(row.get('name', '')),
                'categories': categories,
                'tema': str(row.get('tema', '')),
                'color': str(row.get('color', '')),
                'size': str(row.get('size', ''))
            }
            
            # Format prompt template with cleaned data
            prompt = self.prompts[field].format(**context)
            
            response = self.openai_client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "Sei un copywriter esperto di ecommerce che scrive contenuti per prodotti di carnevale e costumi."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=1000
            )
            
            content = response.choices[0].message.content.strip()
            
            if field == 'url_key':
                content = content.lower().replace(' ', '-')
            
            return content
            
        except Exception as e:
            logging.error(f"Error generating content for {field}: {e}")
            return None

    def process_database(self, limit: int = None):
        """Process the global database and enrich content.
        
        Args:
            limit (int, optional): Maximum number of products to process. If None, process all products.
        """
        try:
            # Load database
            df = pd.read_csv(self.config.output_file, dtype=str)  # Force string type
            total_rows = len(df)
            
            # Apply limit if specified
            if limit and limit > 0:
                df = df.head(limit)
                logging.info(f"Processing first {limit} of {total_rows} products")
            else:
                logging.info(f"Processing all {total_rows} products")
            
            # Process each product
            for idx, row in df.iterrows():
                logging.info(f"Processing product {idx+1}/{len(df)}: {row['sku']}")
                
                for field in self.fields_to_enrich:
                    current_value = str(df.at[idx, field])
                    if pd.isna(current_value) or current_value == '' or current_value.lower() == 'nan':
                        content = self.generate_content(field, row)
                        if content:
                            df.at[idx, field] = str(content)  # Force string type
                            logging.info(f"Generated {field} for {row['sku']}")
                        
                        # Add delay to respect API rate limits
                        time.sleep(1)
                
                # Save progress periodically
                if (idx + 1) % 10 == 0:
                    df.to_csv(self.config.output_file, index=False)
                    logging.info(f"Progress saved after {idx+1} products")
            
            # Final save - make sure we don't overwrite the entire file
            if limit and limit > 0:
                # Save to a test file instead
                test_file = self.config.output_file.replace('.csv', '_test.csv')
                df.to_csv(test_file, index=False)
                logging.info(f"Test results saved to: {test_file}")
            else:
                df.to_csv(self.config.output_file, index=False)
                logging.info("Content enrichment completed")
            
        except Exception as e:
            logging.error(f"Error during content enrichment: {e}")
            raise
