import logging
from config import Config
from content_enricher import ContentEnricher
from rich.logging import RichHandler
import argparse

def main():
    # Setup argument parser
    parser = argparse.ArgumentParser(description='Enrich product catalog with AI-generated content')
    parser.add_argument('--limit', type=int, default=None, help='Number of products to process (default: all)')
    parser.add_argument('--test', action='store_true', help='Run in test mode with sample products')
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[
            RichHandler(show_time=False),
            logging.FileHandler('enrichment.log')
        ]
    )

    try:
        config = Config()
        enricher = ContentEnricher(config)
        
        # Load prompts
        logging.info("Loading prompt templates...")
        enricher.load_prompts()
        
        # Process database
        if args.test:
            logging.info("Running in test mode...")
            enricher.process_database(limit=2)
        else:
            enricher.process_database(limit=args.limit)
        
    except Exception as e:
        logging.error(f"Content enrichment failed: {e}")
        raise

if __name__ == "__main__":
    main()
