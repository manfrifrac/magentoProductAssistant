import pandas as pd
from openai import OpenAI
import logging
from pathlib import Path
import os
from dotenv import load_dotenv
import csv

class ProductEnricher:
    def __init__(self):
        load_dotenv()
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        # Define field mappings from CSV to internal names
        self.field_mappings = {
            'name': ['name', 'product_name', 'Name', 'NOME'],
            'description': ['description', 'product_description', 'Description', 'DESCRIZIONE'],
            'url_key': ['url_key', 'url', 'URL_Key'],
            'short_description': ['short_description', 'short_desc', 'meta_description']
        }
        self.fields_to_enrich = list(self.field_mappings.keys())
        self.prompts = {}
        self.load_prompts()
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s',
            handlers=[
                logging.FileHandler('enrichment.log'),
                logging.StreamHandler()
            ]
        )

    def load_prompts(self):
        """Load prompts from prompts.csv file."""
        try:
            prompts_file = Path(__file__).parent / 'prompts.csv'
            df = pd.read_csv(prompts_file, encoding='utf-8')
            self.prompts = {
                (row['field'], row['supplier']): row['prompt']
                for _, row in df.iterrows()
            }
            logging.info(f"Loaded {len(self.prompts)} prompts from {prompts_file}")
        except Exception as e:
            logging.error(f"Error loading prompts: {e}")
            raise

    def get_prompt(self, field: str, supplier: str = 'any') -> str:
        """Get appropriate prompt for field and supplier."""
        return self.prompts.get((field, supplier)) or self.prompts.get((field, 'any'))

    def improve_text(self, text, field_type, context):
        try:
            prompt_template = self.get_prompt(field_type)
            if not prompt_template:
                logging.warning(f"No prompt found for {field_type}, using original text")
                return text

            # Remove quotes from description if present
            description = context.get('description', '')
            if description.startswith('"') and description.endswith('"'):
                description = description[1:-1]

            # Create context with proper values
            format_context = {
                'description': description,
                'theme': context.get('theme', '').rstrip(','),  # Remove trailing comma
                'color': context.get('color', ''),
                'material': context.get('material', ''),
                'size': context.get('size', '')
            }

            # Log the context being used
            logging.debug(f"Using context for {field_type}: {format_context}")

            try:
                prompt = prompt_template.format(**format_context)
                logging.info(f"Generated prompt for {field_type}: {prompt}")
            except KeyError as e:
                logging.warning(f"Error formatting prompt for {field_type}: {e}")
                return text

            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a professional e-commerce copywriter. Create concise, SEO-friendly content."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=200
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logging.error(f"Error enriching {field_type}: {e}")
            return text

    def find_field_in_df(self, df, field):
        """Find the actual column name in DataFrame for a given field."""
        possible_names = self.field_mappings[field]
        for name in possible_names:
            if name in df.columns:
                return name
        return None

    def enrich_products(self, input_file, output_file, test_mode=False):
        try:
            # Read input CSV
            df = pd.read_csv(input_file)
            logging.info(f"Loaded {len(df)} products from {input_file}")
            logging.info(f"Input columns: {df.columns.tolist()}")

            if test_mode:
                df = df.sample(n=2)
                logging.info("Test mode: Processing 2 random products")

            # Create new columns for enriched content
            for field in self.fields_to_enrich:
                if field not in df.columns:
                    df[field] = None
                    logging.info(f"Created new column: {field}")

            # Process each product
            total = len(df)
            for index, row in df.iterrows():
                logging.info(f"Processing product {index + 1}/{total}: {row.get('sku', 'Unknown SKU')}")
                
                # Parse product context
                try:
                    context = eval(row['product_context']) if pd.notna(row.get('product_context')) else {}
                    logging.info(f"Context for product {row.get('sku', 'Unknown')}: {context}")
                    
                    # Use context description as base text if available
                    description_text = context.get('description', '')
                    theme = context.get('theme', '')
                    color = context.get('color', '')
                    
                    # Generate content for each field
                    df.at[index, 'name'] = self.improve_text(description_text, 'name', context)
                    df.at[index, 'description'] = self.improve_text(description_text, 'description', context)
                    df.at[index, 'url_key'] = self.improve_text(f"{theme}-{color}-{row['sku']}", 'url_key', context)
                    df.at[index, 'short_description'] = self.improve_text(description_text, 'short_description', context)
                    
                    logging.info(f"Generated content for product {row['sku']}:")
                    logging.info(f"Name: {df.at[index, 'name'][:50]}...")
                    logging.info(f"Description: {df.at[index, 'description'][:50]}...")
                    logging.info(f"URL Key: {df.at[index, 'url_key']}")
                    logging.info(f"Short Description: {df.at[index, 'short_description'][:50]}...")
                    
                except Exception as e:
                    logging.warning(f"Error processing product {row.get('sku', 'Unknown')}: {e}")
                    continue

                # Save progress periodically
                if (index + 1) % 10 == 0:
                    # Save without product_context column
                    df.drop(columns=['product_context'], inplace=True)
                    df.to_csv(output_file, index=False)
                    logging.info(f"Progress saved: {index + 1}/{total} products")

            # Final save without product_context column
            df.drop(columns=['product_context'], inplace=True)
            df.to_csv(output_file, index=False)
            logging.info(f"Enrichment completed. Output columns: {df.columns.tolist()}")
            return True

        except Exception as e:
            logging.error(f"Enrichment failed: {e}")
            return False

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Enrich product data with AI-generated content')
    parser.add_argument('--test', action='store_true', help='Run in test mode with 2 random products')
    parser.add_argument('--input', default='data/output/global_database.csv', help='Input CSV file path')
    parser.add_argument('--output', default='data/output/enriched_database.csv', help='Output CSV file path')
    args = parser.parse_args()

    enricher = ProductEnricher()
    enricher.enrich_products(args.input, args.output, args.test)

if __name__ == "__main__":
    main()
