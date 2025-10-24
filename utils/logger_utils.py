# utils/logger_utils.py
import logging
from logging.handlers import TimedRotatingFileHandler
from rich.logging import RichHandler
from config import LOG_FILE
import os
import datetime

# ───────────────────────────────────────────────
# CUSTOM LOG LEVEL
# ───────────────────────────────────────────────
SUCCESS_LEVEL_NUM = 25  # Between INFO (20) and WARNING (30)
logging.addLevelName(SUCCESS_LEVEL_NUM, "SUCCESS")

def success(self, message, *args, **kwargs):
    """Custom success log method."""
    if self.isEnabledFor(SUCCESS_LEVEL_NUM):
        self._log(SUCCESS_LEVEL_NUM, message, args, **kwargs)

logging.Logger.success = success


# ───────────────────────────────────────────────
# LOGGER FACTORY FUNCTION
# ───────────────────────────────────────────────
def get_logger(name: str = None, use_rich: bool = True) -> logging.Logger:
    """
    Create a colorized, rotating, multi-handler logger.
    Logs go both to console (Rich) and to file (rotating).
    """
    logger = logging.getLogger(name or __name__)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # ─────────────── FILE HANDLER (ROTATING, NO DELETION) ───────────────
        log_dir = os.path.dirname(LOG_FILE) or "."
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Rotate daily, but keep ALL old logs (no deletion)
        file_handler = TimedRotatingFileHandler(
            LOG_FILE,
            when="midnight",        # rotate daily at midnight
            interval=1,
            backupCount=0,          # 0 = keep all old logs
            encoding="utf-8",
            delay=False,
            utc=True
        )

        file_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # ─────────────── CONSOLE HANDLER (RICH) ───────────────
        if use_rich:
            console_handler = RichHandler(
                rich_tracebacks=True,
                markup=True,
                show_time=False,
                show_path=False,
                log_time_format="%Y-%m-%d %H:%M:%S",
                keywords=["SUCCESS"]
            )
            console_formatter = logging.Formatter("%(message)s")
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
        else:
            # fallback simple stream handler
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(file_formatter)
            logger.addHandler(stream_handler)

        # ─────────────── STARTUP BANNER ───────────────
        logger.info("Logger initialized with rotating file + rich console output")

    return logger
