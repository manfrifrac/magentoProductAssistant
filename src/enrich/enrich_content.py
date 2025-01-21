import logging
from config import Config
from content_enricher import ContentEnricher
from rich.logging import RichHandler

def main():
    # Setup logging
    # Crea FileHandler senza timestamp
    file_handler = logging.FileHandler('content_enrichment.log', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))  # Rimuove il timestamp

    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[
            RichHandler(show_time=False, rich_tracebacks=True),  # Disabilita i timestamp
            file_handler  # Usa il FileHandler formattato
        ]
    )
    logging.debug("Logging using the Rich library")

    try:
        # Initialize
        config = Config()
        enricher = ContentEnricher(config)
        
        # Load prompts
        logging.info("Loading prompt templates...")
        enricher.load_prompts()
        
        # Process database - limit to exactly 3 random products
        logging.info("Starting content enrichment process...")
        enricher.process_database(limit=3)
        
    except Exception as e:
        logging.error(f"Content enrichment failed: {e}")
        raise

if __name__ == "__main__": 
    main()
