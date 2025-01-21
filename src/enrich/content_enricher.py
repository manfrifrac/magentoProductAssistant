import pandas as pd
import openai
import logging
import csv
from typing import Dict, Optional
from config import Config

class ContentEnricher:
    def __init__(self, config: Config):
        self.config = config
        self.openai_client = openai.OpenAI(api_key=config.openai_api_key)
        self.prompts = {}

    def load_prompts(self) -> None:
        """Load prompts from prompts.csv file."""
        try:
            df = pd.read_csv(
                self.config.prompts_file,
                encoding='utf-8',
                quoting=csv.QUOTE_ALL
            )
            self.prompts = {
                (row['field'], row['supplier']): row['prompt']
                for _, row in df.iterrows()
            }
            logging.info(f"Loaded {len(self.prompts)} prompts")
        except Exception as e:
            logging.error(f"Error loading prompts: {e}")
            raise

    def get_prompt(self, field: str, supplier: str = 'any') -> Optional[str]:
        """Get prompt for field and supplier."""
        return self.prompts.get((field, supplier)) or self.prompts.get((field, 'any'))

    def generate_content(self, field: str, context: Dict) -> Optional[str]:
        """Generate content using OpenAI API."""
        try:
            prompt = self.get_prompt(field)
            if not prompt:
                logging.debug(f"No prompt found for field '{field}'")
                return None

            # Ensure context fields used in the prompt are present
            formatted_prompt = prompt.format(**context)
            logging.debug(f"Formatted prompt for field '{field}': {formatted_prompt}")

            response = self.openai_client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a professional e-commerce copywriter."},
                    {"role": "user", "content": formatted_prompt}
                ],
                temperature=0.7,
                max_tokens=500
            )

            generated_content = response.choices[0].message.content.strip()
            logging.debug(f"Generated content for field '{field}': {generated_content}")
            return generated_content

        except Exception as e:
            logging.error(f"Error generating content for {field}: {e}")
            return None

    def process_database(self, limit=None):
        # Example of using ProductContext and generating content:
        # product_context = ProductContext({"name": "Example Product", "description": "..."})
        # context = product_context.get_context()
        # for field in ["name", "description"]:
        #     content = self.generate_content(field, context)
        #     # ...do something with content...
        pass

