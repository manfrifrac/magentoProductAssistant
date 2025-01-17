import logging
from config import Config
from catalog_processor import CatalogProcessor

def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Get configuration without user input
        config = Config()
        
        # Initialize and run processor
        processor = CatalogProcessor(config)
        processor.load_mapping()
        processor.process_all_catalogs()
        
    except Exception as e:
        logging.error(f"Errore durante l'esecuzione: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
