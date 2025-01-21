import logging
from pathlib import Path
from config import Config
from src.processors.catalog_processor import CatalogProcessor
from rich.logging import RichHandler
import sys

def setup_logging():
    """Configure logging with both file and console output."""
    log_file = Path("catalog_processing.log")
    
    # Set default logging level to INFO but filter progress messages
    logging.getLogger().setLevel(logging.INFO)
    
    # Create custom formatter
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    
    # Configure handlers
    console_handler = RichHandler(rich_tracebacks=True, show_time=False)
    file_handler = logging.FileHandler(log_file)
    
    # Set formatters
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Configure logging
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
    logger = setup_logging()
    logger.info("Starting Magento Product Assistant")
    
    try:
        logger.info("Loading configuration...")
        config = Config()
        
        if not process_catalogs(config, logger):
            logger.error("Catalog processing failed")
            sys.exit(1)
        logger.info("Application completed successfully")
            
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
