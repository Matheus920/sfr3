"""
Centralized logging configuration module.

Import this module in any file that needs logging to ensure consistent configuration.
"""

import logging
from logging.config import fileConfig
import os

# Path to the logging configuration file
LOGGING_CONFIG_PATH = "logging_config.ini"
LOG_LEVEL = logging.INFO


def setup_logging():
    """
    Set up logging configuration for the entire application.

    This function loads logging configuration from the logging_config.ini file
    and ensures that all loggers are set to the appropriate level.
    """
    # Load logging configuration from file
    if os.path.exists(LOGGING_CONFIG_PATH):
        fileConfig(LOGGING_CONFIG_PATH)

        # Set Snowflake connector to INFO to avoid flooding logs
        logging.getLogger("snowflake.connector").setLevel(logging.INFO)

        # Log successful configuration
        root_logger = logging.getLogger()
        root_logger.debug(f"Logging configured from {LOGGING_CONFIG_PATH}")
    else:
        # Default configuration if file doesn't exist
        logging.basicConfig(
            level=LOG_LEVEL,
            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        )
        logging.warning(
            f"Logging config file {LOGGING_CONFIG_PATH} not found. Using default configuration."
        )


def get_logger(name):
    """
    Get a configured logger for the specified module.

    This function ensures that the logger has the appropriate level set.

    Args:
        name: Usually __name__ from the calling module

    Returns:
        logging.Logger: A configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    return logger


# Run setup when the module is imported
setup_logging()
