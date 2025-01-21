import logging
from pathlib import Path
from config import Config
from src.processors.catalog_processor import CatalogProcessor
from rich.logging import RichHandler
import sys
import argparse
import os

def setup_logging():
    """Configure logging with both file and console output."""
    log_file = Path("catalog_processing.log")
    
    logging.getLogger().setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    
    console_handler = RichHandler(rich_tracebacks=True, show_time=False)
    file_handler = logging.FileHandler(log_file)
    
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    logging.basicConfig(
        level=logging.INFO,
        handlers=[console_handler, file_handler]
    )
    
    return logging.getLogger(__name__)

def process_catalogs(config: Config, logger: logging.Logger) -> bool:
    """Process all supplier catalogs."""
    try:
        logger.info("Initializing CatalogProcessor...")
        processor = CatalogProcessor(config)
        
        logger.info("Loading supplier mappings...")
        processor.load_mapping()
        
        logger.info("Starting catalog processing...")
        processor.process_all_catalogs()
        
        logger.info("Catalog processing completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process catalogs: {str(e)}", exc_info=True)
        return False

def main():
    parser = argparse.ArgumentParser(description='Magento Product Assistant')
    parser.add_argument('--enrich', action='store_true', help='Run only the enrichment phase')
    parser.add_argument('--test', action='store_true', help='Run in test mode with 2 random products')
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("Starting Magento Product Assistant")

    try:
        config = Config()

        if args.enrich:
            # Import here to avoid circular imports
            from src.enrich.enrichment import ProductEnricher
            
            input_file = os.path.join('data', 'output', 'global_database.csv')
            output_file = os.path.join('data', 'output', 'enriched_database.csv')
            
            logger.info("Initializing content enrichment...")
            enricher = ProductEnricher()
            
            if args.test:
                logger.info("Running in test mode with 2 random products")
            
            success = enricher.enrich_products(input_file, output_file, test_mode=args.test)
            
            if success:
                logger.info(f"Enrichment completed. Results saved to {output_file}")
            else:
                logger.error("Enrichment process failed")
                sys.exit(1)
        else:
            # Run catalog processing phase
            if not process_catalogs(config, logger):
                logger.error("Catalog processing failed")
                sys.exit(1)
            logger.info("Catalog processing completed successfully")
                
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
