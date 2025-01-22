import pandas as pd
from openai import OpenAI
import logging
from pathlib import Path
import os
from dotenv import load_dotenv
import csv
import ast

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

    def clean_context_string(self, context_str):
        """Clean and validate context string before evaluation."""
        if pd.isna(context_str):
            return "{}"
        
        # Replace smart quotes with straight quotes
        context_str = context_str.replace('"', '"').replace('"', '"')
        context_str = context_str.replace("'", "'").replace("'", "'")
        
        # Ensure it's a valid dictionary string
        try:
            # Try parsing with ast.literal_eval first
            return context_str.strip()
        except:
            logging.warning(f"Invalid context string: {context_str}")
            return "{}"

    def parse_context(self, context_str):
        """Safely parse context string into dictionary."""
        try:
            if pd.isna(context_str):
                return {}
                
            if isinstance(context_str, str):
                cleaned_str = self.clean_context_string(context_str)
                context = ast.literal_eval(cleaned_str)
                return context if isinstance(context, dict) else {}
            elif isinstance(context_str, dict):
                return context_str
            else:
                logging.warning(f"Unexpected context type: {type(context_str)}")
                return {}
        except Exception as e:
            logging.warning(f"Failed to parse context: {str(e)}")
            return {}

    def parse_context_string(self, context_str):
        """Parse context string in format 'description: "TEXT" | key:value | key:value'"""
        if pd.isna(context_str):
            return {}
            
        try:
            # Split by pipe character
            parts = [p.strip() for p in str(context_str).split('|')]
            context = {}
            
            for part in parts:
                if ':' in part:
                    key, value = part.split(':', 1)
                    key = key.strip().lower()
                    value = value.strip()
                    
                    # Clean up values
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    if key == 'theme':
                        value = value.split('/')[0].strip()  # Take first theme if multiple
                    if key == 'color':
                        value = value.split('/')[0].strip()  # Take first color if multiple
                        
                    context[key] = value
                    
                    # Extract additional info from description if needed
                    if key == 'description':
                        desc_lower = value.lower()
                        # Try to extract color if not present
                        if 'color' not in context:
                            for color in ['rosa', 'blu', 'rosso', 'nero', 'bianco']:
                                if color in desc_lower:
                                    context['color'] = color.title()
                                    break
                        # Try to extract theme if not present
                        if 'theme' not in context:
                            themes = {
                                'carnevale': 'Carnevale',
                                'halloween': 'Halloween',
                                'natale': 'Natale',
                                'costume': 'Costumi'
                            }
                            for theme_key, theme_value in themes.items():
                                if theme_key in desc_lower:
                                    context['theme'] = theme_value
                                    break
            
            logging.debug(f"Parsed context: {context}")
            return context
            
        except Exception as e:
            logging.warning(f"Error parsing context string: {str(e)}")
            return {}

    def enrich_products(self, input_file, output_file, test_mode=False):
        try:
            # Read input CSV
            df = pd.read_csv(input_file)
            logging.info(f"Loaded {len(df)} products from {input_file}")
            
            if 'product_context' not in df.columns:
                logging.error("Required column 'product_context' not found in input file")
                return False

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
                try:
                    sku = row.get('sku', 'Unknown SKU')
                    logging.info(f"Processing product {index + 1}/{total}: {sku}")
                    
                    # Parse context with improved logging
                    context = self.parse_context_string(row['product_context'])
                    logging.info(f"Raw context for {sku}: {row['product_context']}")
                    logging.info(f"Parsed context for {sku}: {context}")
                    
                    # Check required fields
                    required_fields = ['description', 'theme', 'color']
                    if not all(field in context for field in required_fields):
                        logging.warning(f"Missing required context fields for {sku}")
                        logging.warning(f"Available fields: {list(context.keys())}")
                        continue
                    
                    # Generate content for each field using context
                    df.at[index, 'name'] = self.improve_text("", 'name', context)
                    df.at[index, 'description'] = self.improve_text("", 'description', context)
                    df.at[index, 'url_key'] = self.improve_text("", 'url_key', context)
                    df.at[index, 'short_description'] = self.improve_text("", 'short_description', context)
                    
                    # Log generated content
                    logging.info(f"Generated content for {sku}:")
                    for field in self.fields_to_enrich:
                        value = df.at[index, field]
                        logging.info(f"{field}: {value[:100]}...")
                        
                except Exception as e:
                    logging.error(f"Error processing product {sku}: {str(e)}")
                    continue

                # Save progress periodically
                if (index + 1) % 10 == 0:
                    # Save progress, dropping product_context if it exists
                    output_df = df.copy()
                    if 'product_context' in output_df.columns:
                        output_df = output_df.drop(columns=['product_context'])
                    output_df.to_csv(output_file, index=False)
                    logging.info(f"Progress saved: {index + 1}/{total} products")

            # Final save
            output_df = df.copy()
            if 'product_context' in output_df.columns:
                output_df = output_df.drop(columns=['product_context'])
            output_df.to_csv(output_file, index=False)
            logging.info(f"Enrichment completed. Output columns: {output_df.columns.tolist()}")
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
