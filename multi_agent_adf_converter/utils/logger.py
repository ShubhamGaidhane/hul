"""Logging configuration using structlog for structured observability."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

import structlog


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Configure structured logging with structlog.

    Args:
        log_level: Python log level string (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional path to a log file. If None, logs to stderr.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        level=level,
        stream=sys.stderr,
    )

    # Build processors list
    processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        processors.append(structlog.processors.JSONRenderer())
        structlog.configure(
            processors=processors,
            wrapper_class=structlog.make_filtering_bound_logger(level),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(
                file=open(log_path, "a", encoding="utf-8")
            ),
            cache_logger_on_first_use=True,
        )
    else:
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True,
                pad_level=False,
                sort_keys=False,
            )
        )
        structlog.configure(
            processors=processors,
            wrapper_class=structlog.make_filtering_bound_logger(level),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
            cache_logger_on_first_use=True,
        )

    # Quiet noisy third-party loggers
    for noisy in ("azure", "azure.identity", "urllib3", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str = "adf_converter") -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance.

    Args:
        name: Logger name, typically __name__

    Returns:
        A structlog BoundLogger
    """
    return structlog.get_logger(name)
