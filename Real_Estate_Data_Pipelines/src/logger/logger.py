# logger_util.py
import logging
from pathlib import Path
from datetime import datetime
import inspect

class LoggerFactory:
    @staticmethod
    def create_logger(log_dir: str):
        """
        Creates a logger whose file name is auto-derived from the calling script.
        Example filename: property_mart_builder_20251205.log
        """
        # Detect the calling file name
        caller_frame = inspect.stack()[1]
        caller_file = Path(caller_frame.filename).stem

        # Build log dir
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        # Final log file path
        log_file = log_path / f"{caller_file}_{datetime.now().strftime('%Y%m%d')}.log"

        # Create logger
        logger = logging.getLogger(caller_file)
        logger.setLevel(logging.INFO)

        # Prevent duplicate handlers when re-importing
        if not logger.handlers:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            console_handler = logging.StreamHandler()

            formatter = logging.Formatter(
                "%(asctime)s — %(name)s — %(levelname)s — %(message)s",
                "%Y-%m-%d %H:%M:%S"
            )

            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        return logger
