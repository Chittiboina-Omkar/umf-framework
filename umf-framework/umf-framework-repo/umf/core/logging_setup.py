"""Crash-safe, structured logging for the framework.

Logging is not an afterthought here; it is a first-class requirement. Every run
must leave a complete, queryable trail even if the job crashes. This module
configures Python's standard :mod:`logging` so that:

    * every record is emitted as a single JSON line (easy to ship to a log table
      or a file and query later),
    * a run identifier and the framework version are attached to every record
      (so you can always tie a log line back to a specific run and build),
    * secrets are masked before they can reach a handler, and
    * configuration is idempotent, so calling it twice does not double every log
      line.

What this module does *not* do:
    It does not write to a database log table. Persisting structured events to
    the control / log tables (``run_log``, ``step_log``, ``error_log`` and so
    on) is a separate concern handled by the run context and the logging sinks
    added in later parts. This module is the in-process logging backbone those
    sinks build on. Keeping the two separate means a failure to write to the log
    table can itself be logged to the console, never swallowed.

Standard library only, so it imports and runs anywhere.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from umf.core.masking import mask_structure, mask_text

#: The logger name that the whole framework logs under. Using one named logger
#: (rather than the root logger) means the framework's logging configuration
#: never fights with the host application's or the platform's own logging.
LOGGER_NAME = "umf"

#: Module-level register of secret values to scrub from free-text messages.
#: Populated by :func:`register_secrets`. Kept deliberately small and only ever
#: holds values already present in the process; nothing is persisted.
_KNOWN_SECRETS: set[str] = set()


class JsonFormatter(logging.Formatter):
    """Format each log record as a single JSON object on one line.

    One JSON object per line ("JSON Lines") is the most portable structured log
    format: a file of them can be loaded directly into a table, and a log
    collector can parse each line independently. Any extra fields attached to a
    record via ``logger.info(msg, extra={...})`` are merged into the object.

    The formatter also masks secrets: the message text is scrubbed of any
    registered secret value, and any structured ``extra`` payload is passed
    through :func:`~umf.core.masking.mask_structure`.
    """

    #: Attributes that :mod:`logging` puts on every record. Anything on a record
    #: that is not in this set is treated as a user-supplied ``extra`` field and
    #: included in the JSON output.
    _RESERVED = set(
        vars(logging.makeLogRecord({})).keys()
    ) | {"message", "asctime", "taskName"}

    def format(self, record: logging.LogRecord) -> str:
        """Render a log record as a JSON line.

        Args:
            record: The log record to format.

        Returns:
            A JSON string with no embedded newlines.
        """
        # Scrub the human message of any known secret values.
        message = mask_text(record.getMessage(), _KNOWN_SECRETS)

        payload: Dict[str, Any] = {
            # ISO 8601 UTC timestamp; UTC avoids timezone ambiguity in logs.
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
        }

        # Attach exception information if present, as text (never raw data).
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        # Merge any extra structured fields, with secrets masked. We copy so the
        # caller's dictionary is never mutated.
        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in self._RESERVED and not key.startswith("_")
        }
        if extras:
            payload.update(mask_structure(extras))

        # ``default=str`` guarantees serialisation never fails on an odd type
        # (for example an enum or a datetime); logging must not raise.
        return json.dumps(payload, default=str, ensure_ascii=False)


def register_secrets(values: Iterable[str]) -> None:
    """Register secret values so they are scrubbed from free-text log messages.

    Call this once the framework has resolved secret values it must use (for
    example a database password fetched from a vault reference). The values are
    held only in memory for the life of the process and are used solely to
    prevent accidental leakage into log text; they are never written anywhere.

    Args:
        values: The concrete secret strings to scrub from future log messages.
    """
    for value in values:
        if value and len(value) >= 4:
            _KNOWN_SECRETS.add(value)


def init_logging(level: int = logging.INFO, *, stream: Optional[Any] = None) -> logging.Logger:
    """Configure and return the framework's logger.

    Safe to call more than once: existing handlers added by this function are
    removed first, so repeated calls (for example once per run in a long-lived
    notebook session) do not multiply log output.

    The logger does not propagate to the root logger, so the framework's JSON
    lines never get duplicated or reformatted by the host application's logging.

    Args:
        level: The minimum level to emit (for example ``logging.INFO``).
        stream: The stream to write to. Defaults to standard output, which every
            platform captures into its own driver / job logs.

    Returns:
        The configured framework logger.
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False

    # Remove any handler we added before, identified by a marker attribute, so
    # that re-initialising does not stack duplicate handlers.
    for handler in list(logger.handlers):
        if getattr(handler, "_umf_handler", False):
            logger.removeHandler(handler)

    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter())
    # Mark the handler so we can recognise and replace it on re-init.
    handler._umf_handler = True  # type: ignore[attr-defined]
    logger.addHandler(handler)

    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger under the framework's namespace.

    Using child loggers (for example ``umf.core.config_loader``) keeps the
    source of each line visible while still inheriting the framework's single
    handler and formatter.

    Args:
        name: An optional dotted sub-name. If given, the returned logger is
            ``umf.<name>``; otherwise the top-level framework logger is
            returned.

    Returns:
        The requested logger.
    """
    if name:
        return logging.getLogger(f"{LOGGER_NAME}.{name}")
    return logging.getLogger(LOGGER_NAME)
