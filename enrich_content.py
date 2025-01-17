import logging
from config import Config
from content_enricher import ContentEnricher

def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('content_enrichment.log'),
            logging.StreamHandler()
        ]
    )

    try:
        # Initialize
        config = Config()
        enricher = ContentEnricher(config)
        
        # Load prompts
        logging.info("Loading prompt templates...")
        enricher.load_prompts()
        
        # Process database - limit to 10 products for testing
        logging.info("Starting content enrichment process...")
        enricher.process_database(limit=10)
        
    except Exception as e:
        logging.error(f"Content enrichment failed: {e}")
        raise

if __name__ == "__main__":
    main()
