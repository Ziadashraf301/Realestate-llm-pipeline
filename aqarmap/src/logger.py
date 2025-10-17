import logging

class real_estate_logger:
    
    def __init__(self, log_file='scraper.log'):
        """Initialize the logger"""
        self._setup_logging(log_file)

    def _setup_logging(self, log_file):
        """Setup logging to file and console"""
        # Create logger
        self.logger = logging.getLogger('RealEstateScraper')
        self.logger.setLevel(logging.INFO)
        
        # Clear any existing handlers
        self.logger.handlers = []
        
        # File handler
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.logger.info("="*60)
        self.logger.info("Log File initialized")
        self.logger.info(f"Log file: {log_file}")
        self.logger.info("="*60)