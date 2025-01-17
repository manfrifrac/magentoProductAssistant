import pandas as pd
import openai
import logging
import time
import numpy as np  # Aggiungi import per random
from typing import Dict, Optional
from config import Config

class ContentEnricher:
    def __init__(self, config: Config):
        self.config = config
        self.prompts: Dict[str, str] = {}
        self.openai_client = openai.OpenAI(api_key=config.openai_api_key)
        self.fields_to_enrich = ['name', 'description', 'short_description', 'url_key']
        self.html_template = """
<div class="product-description">
    <h2>Descrizione del Prodotto</h2>
    <div class="overview">
        <p>{overview}</p>
    </div>
    
    <h3>Caratteristiche Principali</h3>
    <ul>
        <li>{feature1}</li>
        <li>{feature2}</li>
        <li>{feature3}</li>
    </ul>
    
    <h3>Materiali e Qualità</h3>
    <p>{materials}</p>
    
    <h3>Utilizzo Consigliato</h3>
    <p>{usage}</p>
</div>
"""

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

    def format_html_description(self, raw_content: str) -> str:
        """Formatta il contenuto in HTML se non rispetta il template."""
        try:
            # Se il contenuto ha già i tag HTML corretti, lo restituisce
            if all(tag in raw_content.lower() for tag in ['<div', '<h2', '<h3', '<p', '<ul', '<li']):
                return raw_content
                
            # Altrimenti, estrae le informazioni e le formatta
            lines = raw_content.split('\n')
            overview = lines[0] if lines else "Scopri questo fantastico prodotto"
            features = [l.strip('- ').strip() for l in lines[1:4] if l.strip()]
            while len(features) < 3:
                features.append("Versatile e di qualità")
                
            return self.html_template.format(
                overview=overview,
                feature1=features[0],
                feature2=features[1],
                feature3=features[2],
                materials="Realizzato con materiali di alta qualità selezionati per garantire durata e comfort.",
                usage="Perfetto per feste in maschera, carnevale, halloween e ogni occasione che richieda un costume originale."
            )
        except Exception as e:
            logging.error(f"Error formatting HTML description: {e}")
            return raw_content

    def generate_content(self, field: str, row: pd.Series) -> Optional[str]:
        """Generate content for a specific field using OpenAI."""
        if field not in self.prompts:
            return None
            
        try:
            # Get only additional context
            additional_context = str(row.get('additional_context', ''))
            if not additional_context or additional_context.lower() == 'nan':
                logging.warning(f"No additional context found for SKU {row.get('sku', 'unknown')}")
                return None
                
            # Prepare context with only additional_context
            context = {
                'additional_context': additional_context
            }
            
            # Format prompt template
            prompt = self.prompts[field].format(**context)
            
            response = self.openai_client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "Sei un copywriter italiano esperto di ecommerce che scrive contenuti esclusivamente in italiano per prodotti di carnevale e costumi."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=1000
            )
            
            content = response.choices[0].message.content.strip()
            
            # Log del contenuto generato
            logging.info(f"\nGenerated {field} for SKU {row.get('sku')}:\n{'='*50}\n{content}\n{'='*50}\n")
            
            if field == 'description':
                content = self.format_html_description(content)
                # Log della versione formattata HTML se modificata
                if content != response.choices[0].message.content.strip():
                    logging.info(f"\nFormatted HTML description for SKU {row.get('sku')}:\n{'='*50}\n{content}\n{'='*50}\n")
            elif field == 'url_key':
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
            
            # Select 2 random products
            test_limit = 2
            if (total_rows > test_limit):
                random_indices = np.random.choice(total_rows, test_limit, replace=False)
                df = df.iloc[random_indices].copy()
            else:
                df = df.copy()
                
            logging.info(f"Processing {len(df)} random products from {total_rows} total products")
            logging.info(f"Selected SKUs: {', '.join(df['sku'].tolist())}")
            
            # Process each product
            for idx, row in df.iterrows():
                logging.info(f"\nProcessing product {idx+1}/{len(df)}: {row['sku']}\n{'-'*50}")
                
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
                # Remove additional_context before saving
                df_output = df.drop(columns=['additional_context'], errors='ignore')
                df_output.to_csv(test_file, index=False)
                logging.info(f"Test results saved to: {test_file}")
            else:
                df_output = df.drop(columns=['additional_context'], errors='ignore')
                df_output.to_csv(self.config.output_file, index=False)
                logging.info("Content enrichment completed")
            
        except Exception as e:
            logging.error(f"Error during content enrichment: {e}")
            raise
