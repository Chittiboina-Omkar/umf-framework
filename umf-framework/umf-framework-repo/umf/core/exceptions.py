"""The framework's exception hierarchy.

Every error the framework raises on purpose is a subclass of :class:`UmfError`.
Each carries structured fields (category, severity, whether a retry could help,
and a free-form context dictionary) so that:

    * the logging layer can write a consistent, queryable error record, and
    * calling code can decide what to do (retry a transient connection error,
      but never retry a bad-config error) without parsing error text.

Why not just raise ``ValueError``?
    A plain exception tells you *what* line failed but not *what kind* of
    problem it was or whether retrying is sensible. In a long-running data
    pipeline that distinction is the difference between an automatic recovery
    and silently repeating a doomed operation. Structured errors make triage and
    alerting reliable.

Security note:
    Never put raw data values or secrets into an exception message or context.
    Error records are logged, and logs must contain metadata, not customer data.
    Use counts, column names and identifiers instead.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from umf.core.constants import ErrorCategory, Severity


class UmfError(Exception):
    """Base class for every error raised deliberately by the framework.

    Attributes:
        message: A short, human-readable description, safe to show a user. Must
            not contain data values or secrets.
        category: The coarse :class:`ErrorCategory` used for triage and alerts.
        severity: How serious the error is (:class:`Severity`).
        retryable: Whether retrying the same operation could plausibly succeed.
            Connection blips are retryable; a missing config value is not.
        context: Extra structured, non-sensitive details (for example
            ``{"table": "sales", "column": "amount"}``) that help debugging.
        cause: The original exception, if this one wraps another.
    """

    #: Default category for this class. Subclasses override it.
    default_category: ErrorCategory = ErrorCategory.INTERNAL
    #: Default severity for this class. Subclasses may override it.
    default_severity: Severity = Severity.ERROR
    #: Default retry hint for this class.
    default_retryable: bool = False

    def __init__(
        self,
        message: str,
        *,
        category: Optional[ErrorCategory] = None,
        severity: Optional[Severity] = None,
        retryable: Optional[bool] = None,
        context: Optional[Dict[str, Any]] = None,
        cause: Optional[BaseException] = None,
    ) -> None:
        """Create a structured framework error.

        Args:
            message: Short, non-sensitive description of the problem.
            category: Override the class default category.
            severity: Override the class default severity.
            retryable: Override the class default retry hint.
            context: Non-sensitive structured details for debugging.
            cause: The underlying exception being wrapped, if any.
        """
        super().__init__(message)
        self.message = message
        self.category = category or self.default_category
        self.severity = severity or self.default_severity
        self.retryable = self.default_retryable if retryable is None else retryable
        self.context = dict(context or {})
        self.cause = cause
        # Preserve the original traceback chain so debuggers show the root cause.
        if cause is not None:
            self.__cause__ = cause

    def to_dict(self) -> Dict[str, Any]:
        """Return a flat, JSON-serialisable view of the error.

        This is the shape written to the error log. It deliberately excludes any
        data values.

        Returns:
            A dictionary with the error's type, message, category, severity,
            retry hint and context.
        """
        return {
            "error_type": type(self).__name__,
            "message": self.message,
            "category": str(self.category),
            "severity": str(self.severity),
            "retryable": self.retryable,
            "context": self.context,
            "cause": None if self.cause is None else repr(self.cause),
        }

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        """Return a concise one-line form, useful in logs and tracebacks."""
        ctx = f" | context={self.context}" if self.context else ""
        return f"[{self.category}/{self.severity}] {self.message}{ctx}"


class ConfigError(UmfError):
    """A configuration file is missing, unreadable or structurally wrong.

    This is not the same as :class:`ValidationError`: a ``ConfigError`` means we
    could not even load or parse the config, while a ``ValidationError`` means we
    loaded it but its contents are invalid.
    """

    default_category = ErrorCategory.CONFIG
    default_severity = Severity.CRITICAL
    default_retryable = False


class ValidationError(UmfError):
    """The loaded config is syntactically fine but its contents are invalid.

    Typical causes: a mandatory field is missing, a value is not one of the
    allowed options, or a combination of values is contradictory (for example a
    CDC load with no ordering column).

    This error can carry many individual problems at once so the user sees every
    issue in one pass instead of fixing them one at a time.

    Attributes:
        problems: A list of individual, human-readable problem descriptions.
    """

    default_category = ErrorCategory.CONFIG
    default_severity = Severity.CRITICAL
    default_retryable = False

    def __init__(self, message: str, *, problems: Optional[list] = None, **kwargs: Any) -> None:
        """Create a validation error.

        Args:
            message: A short summary (for example "config validation failed").
            problems: The list of individual problems found.
            **kwargs: Passed through to :class:`UmfError`.
        """
        super().__init__(message, **kwargs)
        self.problems = list(problems or [])
        # Surface the problem list in the context so it is logged too.
        self.context.setdefault("problem_count", len(self.problems))
        self.context.setdefault("problems", self.problems)


class PlatformError(UmfError):
    """The runtime platform could not be identified or is unsupported.

    Raised, for example, when the config says ``platform = AUTO`` but detection
    returns ``UNKNOWN`` and no explicit platform was provided.
    """

    default_category = ErrorCategory.PLATFORM
    default_severity = Severity.CRITICAL
    default_retryable = False


class ConnectionError_(UmfError):
    """Could not reach a source or target system.

    Named with a trailing underscore to avoid shadowing Python's built-in
    ``ConnectionError``; import it as ``from umf.core.exceptions import
    ConnectionError_`` when you need the framework's version.
    """

    default_category = ErrorCategory.CONNECTION
    default_severity = Severity.ERROR
    default_retryable = True  # a connection blip is often worth one retry


class AuthError(UmfError):
    """Authentication or authorization failed.

    Not retryable by default: retrying with the same bad credentials or missing
    grant will fail again. A human needs to fix access first.
    """

    default_category = ErrorCategory.AUTH
    default_severity = Severity.CRITICAL
    default_retryable = False


class SchemaError(UmfError):
    """A schema or type-mapping problem was detected.

    Includes the case where a source type cannot be represented exactly in the
    target and no handling decision has been configured yet. In that situation
    the correct behaviour is to fail the affected table and surface the details,
    never to change the data silently.
    """

    default_category = ErrorCategory.SCHEMA
    default_severity = Severity.ERROR
    default_retryable = False


class DataQualityError(UmfError):
    """A data-quality rule failed with an action of 'fail the job'."""

    default_category = ErrorCategory.DQ
    default_severity = Severity.ERROR
    default_retryable = False


class ReconciliationError(UmfError):
    """Reconciliation did not pass, so the target must not be committed.

    This is the guard that enforces "no bad or partial data reaches the target":
    when recon fails, the staged data is discarded and the watermark is held.
    """

    default_category = ErrorCategory.RECON
    default_severity = Severity.CRITICAL
    default_retryable = False
