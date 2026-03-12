import logging
import sys
from pathlib import Path


class LoggerFactory:
    LOG_DIR = Path("logs")

    @classmethod
    def get_logger(cls, name: str, logfile: str) -> logging.Logger:
        logger = logging.getLogger(name)

        # Prevent duplicate handlers if imported multiple times
        if logger.handlers:
            return logger

        logger.setLevel(logging.INFO)

        cls.LOG_DIR.mkdir(exist_ok=True)

        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )

        # File handler
        fh = logging.FileHandler(cls.LOG_DIR / logfile, mode="a")
        fh.setFormatter(formatter)

        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

        return logger