"""Resolving secrets by reference, never by value.

A core rule of the framework is that the config never contains a raw secret.
Instead it contains a *reference* such as ``env://DB_PASSWORD`` or
``databricks://my-scope/db-password``. At run time, a resolver turns the
reference into the actual value, uses it to connect, and immediately registers
it with the logging layer so it can never appear in a log line.

This module defines:

    * :class:`SecretRef`, which parses and represents a reference, and
    * :class:`SecretResolver`, the interface every backend implements.

The concrete resolvers divide into two kinds:
    * a portable, standard-library resolver that reads from environment
      variables (:mod:`umf.secrets.env_resolver`), which works everywhere and is
      ideal for development and tests; and
    * cloud vault resolvers (Databricks secret scopes, Azure Key Vault, AWS
      Secrets Manager, GCP Secret Manager) that are added in a later part,
      because they require each cloud's SDK. They all implement this same
      interface, so switching backends is a matter of the reference scheme, not
      of code changes elsewhere.

Why references, not values:
    Keeping only references in the config means the config is safe to store,
    version and show in a UI; the sensitive value lives only in the platform's
    own secret store and only ever exists in memory for the moment it is needed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from umf.core.exceptions import UmfError
from umf.core.constants import ErrorCategory, Severity


class SecretError(UmfError):
    """A secret reference could not be parsed or resolved.

    Not retryable: a bad reference or a missing secret will not fix itself on a
    retry; a human must correct the reference or create the secret.
    """

    default_category = ErrorCategory.AUTH
    default_severity = Severity.CRITICAL
    default_retryable = False


@dataclass(frozen=True)
class SecretRef:
    """A parsed reference to a secret, of the form ``scheme://path``.

    Examples:
        ``env://DB_PASSWORD``
            Read the environment variable ``DB_PASSWORD``.
        ``databricks://sales-scope/db-password``
            Read key ``db-password`` from Databricks secret scope
            ``sales-scope``.
        ``azure-kv://my-vault/db-password``
            Read secret ``db-password`` from Azure Key Vault ``my-vault``.

    Attributes:
        scheme: The resolver scheme (the part before ``://``), lower-cased.
        path: Everything after ``://``. Its meaning depends on the scheme.
    """

    scheme: str
    path: str

    @staticmethod
    def parse(reference: str) -> "SecretRef":
        """Parse a ``scheme://path`` reference string.

        Args:
            reference: The reference to parse.

        Returns:
            The parsed :class:`SecretRef`.

        Raises:
            SecretError: If the reference is empty or not in ``scheme://path``
                form.
        """
        if not reference or "://" not in reference:
            raise SecretError(
                "Secret reference must look like 'scheme://path' "
                "(for example 'env://DB_PASSWORD').",
                context={"reference": _safe_ref(reference)},
            )
        scheme, _, path = reference.partition("://")
        scheme = scheme.strip().lower()
        path = path.strip()
        if not scheme or not path:
            raise SecretError(
                "Secret reference is missing a scheme or a path.",
                context={"reference": _safe_ref(reference)},
            )
        return SecretRef(scheme=scheme, path=path)

    def split_path(self, parts: int = 2) -> list:
        """Split the path into the given number of parts on '/'.

        Many schemes use a ``container/name`` path (for example
        ``scope/key``). This helper splits it and validates the count.

        Args:
            parts: The exact number of parts expected.

        Returns:
            The list of path parts.

        Raises:
            SecretError: If the path does not split into exactly ``parts``.
        """
        pieces = self.path.split("/")
        if len(pieces) != parts:
            raise SecretError(
                f"Secret reference path '{self.path}' must have {parts} parts "
                f"separated by '/'.",
                context={"scheme": self.scheme},
            )
        return pieces


class SecretResolver:
    """Interface for turning a :class:`SecretRef` into a secret value.

    Each resolver handles one scheme. The resolver registry
    (:mod:`umf.secrets.registry`) routes a reference to the resolver whose
    scheme matches.
    """

    #: The scheme this resolver handles (for example ``"env"``). Subclasses set
    #: this.
    scheme: str = ""

    def resolve(self, ref: SecretRef) -> str:
        """Return the secret value for a reference.

        Implementations must not log the value and must raise
        :class:`SecretError` (not a generic exception that might include the
        value) on failure.

        Args:
            ref: The parsed reference to resolve.

        Returns:
            The secret value.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError


def _safe_ref(reference: object) -> str:
    """Return a reference string that is safe to put in an error message.

    A reference is a pointer, not a secret, so it is safe to show. This helper
    only guards against a non-string sneaking in.

    Args:
        reference: The reference value.

    Returns:
        A string form of the reference.
    """
    return str(reference)
