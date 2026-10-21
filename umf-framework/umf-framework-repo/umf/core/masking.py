"""Redaction of secrets and sensitive values before anything is logged.

Everything the framework logs must be safe to keep: counts, identifiers,
column names and timings, but never passwords, tokens, keys or customer data
values. This module provides one place that knows how to redact a value or walk
a nested structure (as a config is) and replace anything that looks sensitive.

Why centralise this:
    If each call site decided for itself what to hide, one forgotten spot would
    leak a secret into a log forever. By routing all logging and all config
    snapshots through :func:`mask_structure`, redaction is applied consistently.

How it decides what is sensitive:
    A key is considered sensitive if its name contains any of a small set of
    tokens (``password``, ``secret``, ``token``, ``key``, and so on). This is a
    deliberately conservative, name-based rule: it may over-redact a harmless
    key named ``public_key_name``, which is acceptable, but it will not under-
    redact a field named ``db_password``. The set is configurable so a project
    can extend it.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Set

#: The placeholder shown in place of a redacted value.
REDACTED = "***REDACTED***"

#: Substrings that, when found in a key name (case-insensitive), mark the
#: corresponding value as sensitive. Kept small and obvious on purpose.
DEFAULT_SENSITIVE_TOKENS: Set[str] = {
    "password",
    "passwd",
    "secret",
    "token",
    "apikey",
    "api_key",
    "access_key",
    "secret_key",
    "private_key",
    "credential",
    "credentials",
    "sas",             # shared access signature (Azure)
    "connection_string",
    "conn_str",
    "auth",
}


def _key_is_sensitive(key: str, tokens: Set[str]) -> bool:
    """Return True if a key name indicates a sensitive value.

    Args:
        key: The dictionary key being examined.
        tokens: The set of lower-case substrings that mark sensitivity.

    Returns:
        True if the lower-cased key contains any sensitive token.
    """
    lowered = key.lower()
    return any(token in lowered for token in tokens)


def mask_structure(value: Any, *, extra_tokens: Iterable[str] = ()) -> Any:
    """Return a deep copy of ``value`` with sensitive fields redacted.

    The function recurses through dictionaries and lists. For a dictionary, any
    value whose key name looks sensitive is replaced with :data:`REDACTED`;
    other values are recursed into. Scalars are returned unchanged. The original
    structure is never modified.

    This is intended for config snapshots and any structured object that will be
    logged. It does not attempt to redact secrets that appear as bare scalars
    with no identifying key, because there is no reliable way to recognise those
    without false positives; the framework's rule is that secrets only ever
    appear as values under clearly named keys (and, in practice, only as vault
    references, not raw secrets).

    Args:
        value: The structure to copy and redact. Usually a config dictionary.
        extra_tokens: Additional sensitive key substrings for this call, on top
            of :data:`DEFAULT_SENSITIVE_TOKENS`.

    Returns:
        A redacted deep copy of ``value``.

    Example:
        >>> mask_structure({"host": "db1", "password": "hunter2"})
        {'host': 'db1', 'password': '***REDACTED***'}
    """
    tokens = set(DEFAULT_SENSITIVE_TOKENS) | {t.lower() for t in extra_tokens}
    return _mask(value, tokens)


def _mask(value: Any, tokens: Set[str]) -> Any:
    """Recursive worker for :func:`mask_structure`.

    Args:
        value: The current node in the structure.
        tokens: The resolved set of sensitive key substrings.

    Returns:
        A redacted copy of ``value``.
    """
    if isinstance(value, dict):
        result: Dict[Any, Any] = {}
        for key, item in value.items():
            if isinstance(key, str) and _key_is_sensitive(key, tokens):
                result[key] = REDACTED
            else:
                result[key] = _mask(item, tokens)
        return result
    if isinstance(value, list):
        return [_mask(item, tokens) for item in value]
    if isinstance(value, tuple):
        # Preserve tuple type for fidelity when snapshotting.
        return tuple(_mask(item, tokens) for item in value)
    return value


def mask_text(text: str, secrets: Iterable[str]) -> str:
    """Replace any occurrence of a known secret value inside free text.

    This is a last line of defence for messages that are assembled from strings
    where a secret might have leaked in (for example a driver error message that
    echoes a connection string). Provide the set of secret values currently in
    use and any of them found in the text is replaced with :data:`REDACTED`.

    Args:
        text: The text to scrub.
        secrets: The concrete secret values to look for and remove. Empty or
            very short values are ignored to avoid redacting common substrings.

    Returns:
        The text with known secrets replaced by :data:`REDACTED`.
    """
    scrubbed = text
    for secret in secrets:
        # Ignore empty or trivially short values: replacing a 1-2 character
        # string would corrupt the message and redact harmless text.
        if secret and len(secret) >= 4:
            scrubbed = scrubbed.replace(secret, REDACTED)
    return scrubbed
