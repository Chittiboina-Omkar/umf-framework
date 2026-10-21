"""Load, resolve and validate a JSON configuration.

This module turns a JSON config (written by the UI, or by hand) into a
validated, ready-to-run configuration, and it enforces the rules you set:

    * A field whose wrong default could harm data is **mandatory**: if it is
      missing, validation fails before anything runs, with a clear message.
    * A field with a safe default may be left empty, but every such empty field
      is collected into a single **confirmation list** so the user sees exactly
      what defaults will be used and can approve them once. At run time no
      question is ever asked; confirmation happens here, during setup / save.
    * Values are checked against their allowed set, so an unknown option fails
      immediately rather than deep inside a job.

The result is a :class:`ValidationResult` holding the resolved config (with safe
defaults filled in), the list of hard errors, and the list of confirmations.
The caller decides what to do with it:

    * In the UI, show the errors (must fix) and the confirmations (approve
      once).
    * For an unattended / scheduled run, require that the stored config already
      has no outstanding confirmations, so the run proceeds with zero prompts.

Standard library only; this module never touches Spark or any network.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from umf.core.config_schema import FieldSpec, schema
from umf.core.constants import Environment, FieldRisk
from umf.core.exceptions import ConfigError, ValidationError
from umf.core.logging_setup import get_logger

_log = get_logger("core.config_loader")

#: A sentinel meaning "this field was not present in the config at all", which
#: is different from a field present but set to ``null``.
_MISSING = object()


@dataclass
class Confirmation:
    """One empty, safe field whose default will be used, pending approval.

    Attributes:
        path: Dotted path of the field (for example ``"raw.write_mode"``).
        default: The default value that will be applied.
        reason: A short, plain explanation of what that default means.
        description: The field's user-facing description.
    """

    path: str
    default: Any
    reason: str
    description: str

    def message(self) -> str:
        """Return a one-line prompt describing the default that will be used.

        Returns:
            Text suitable for a confirmation list in the UI.
        """
        return (
            f"'{self.path}' is empty -> will use default "
            f"({self.reason}). {self.description}"
        )


@dataclass
class ValidationResult:
    """The outcome of validating a config.

    Attributes:
        resolved: The config with safe defaults filled in. Only meaningful when
            there are no hard errors.
        errors: Hard problems that block the run until fixed.
        confirmations: Empty safe fields whose defaults need one approval.
    """

    resolved: Dict[str, Any]
    errors: List[str] = field(default_factory=list)
    confirmations: List[Confirmation] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Whether the config is free of hard errors.

        Returns:
            True if there are no blocking errors. Confirmations may still be
            present and, depending on the run mode, may still need approval.
        """
        return not self.errors

    def raise_if_invalid(self) -> None:
        """Raise a :class:`ValidationError` if there are any hard errors.

        Raises:
            ValidationError: Carrying the full list of problems, so the user
                sees every issue at once.
        """
        if self.errors:
            raise ValidationError(
                "Config validation failed with "
                f"{len(self.errors)} problem(s).",
                problems=list(self.errors),
            )


def _get_by_path(config: Dict[str, Any], path: str) -> Any:
    """Read a value from a nested config by dotted path.

    Args:
        config: The config dictionary.
        path: A dotted path such as ``"target.table"``.

    Returns:
        The value at the path, or the private ``_MISSING`` sentinel if any part
        of the path is absent. A present-but-null value returns ``None``, which
        is deliberately distinct from missing.
    """
    node: Any = config
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return _MISSING
        node = node[part]
    return node


def _set_by_path(config: Dict[str, Any], path: str, value: Any) -> None:
    """Write a value into a nested config by dotted path, creating parents.

    Args:
        config: The config dictionary to modify in place.
        path: A dotted path such as ``"raw.retention_runs"``.
        value: The value to set.
    """
    parts = path.split(".")
    node = config
    for part in parts[:-1]:
        existing = node.get(part)
        if not isinstance(existing, dict):
            existing = {}
            node[part] = existing
        node = existing
    node[parts[-1]] = value


def _is_empty(value: Any) -> bool:
    """Decide whether a value counts as "empty" for validation.

    Empty means: missing entirely, ``None``, an empty string (after trimming),
    or an empty list / dict. Zero and ``False`` are **not** empty, because they
    are legitimate, meaningful values (for example a decimal tolerance of zero).

    Args:
        value: The value to test.

    Returns:
        True if the value should be treated as not supplied.
    """
    if value is _MISSING or value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, (list, dict, tuple, set)) and len(value) == 0:
        return True
    return False


def _check_allowed(spec: FieldSpec, value: Any) -> Optional[str]:
    """Validate a value against a field's allowed set, if it has one.

    Args:
        spec: The field being validated.
        value: The non-empty value supplied for it.

    Returns:
        ``None`` if the value is acceptable, otherwise a human-readable problem
        description.
    """
    allowed = spec.allowed
    if allowed is None:
        return None

    # Enum type: check case-insensitively via the enum's coerce helper.
    if isinstance(allowed, type):
        try:
            allowed.coerce(value)  # type: ignore[attr-defined]
            return None
        except Exception as exc:
            return str(exc)

    # Explicit sequence of allowed values: compare case-insensitively for
    # strings, exactly otherwise.
    options = list(allowed)
    if isinstance(value, str):
        lowered = {str(o).lower() for o in options}
        if value.strip().lower() in lowered:
            return None
    elif value in options:
        return None
    return (
        f"'{spec.path}' has invalid value '{value}'. "
        f"Allowed: {', '.join(str(o) for o in options)}"
    )


def validate_config(config: Dict[str, Any]) -> ValidationResult:
    """Validate a config dictionary against the field registry.

    The function walks every known field, decides whether it applies to this
    config, and then:

        * for a mandatory (HIGH-risk) field that is empty, records a hard error;
        * for a safe field that is empty, fills in its default and records a
          confirmation;
        * for any supplied value, checks it against the field's allowed set.

    It never mutates the caller's dictionary; it works on a deep copy and
    returns that copy as :attr:`ValidationResult.resolved`.

    Args:
        config: The parsed JSON config.

    Returns:
        A :class:`ValidationResult` with the resolved config, any hard errors,
        and any confirmations.
    """
    # Deep copy via JSON round-trip so defaults we fill in never leak back into
    # the caller's object. The config is plain JSON data, so this is safe and
    # also validates that the structure is JSON-serialisable.
    resolved: Dict[str, Any] = json.loads(json.dumps(config))

    errors: List[str] = []
    confirmations: List[Confirmation] = []

    for spec in schema():
        # Skip fields that are not relevant to this particular run.
        if not spec.applies(resolved):
            continue

        value = _get_by_path(resolved, spec.path)
        empty = _is_empty(value)

        if empty:
            if spec.is_mandatory:
                example = (
                    f" Example: {spec.example}" if spec.example is not None else ""
                )
                errors.append(
                    f"Missing required field '{spec.path}': {spec.description}"
                    + example
                )
            else:
                # Safe field: apply the documented default and record it for the
                # one-time confirmation list.
                _set_by_path(resolved, spec.path, spec.default)
                if spec.risk == FieldRisk.SAFE:
                    # No confirmation is needed when the user explicitly supplied
                    # a value that is already the default (for example an empty
                    # partition list meaning "no partitioning"). A confirmation
                    # is only for a default the user did not choose: an absent
                    # field, or an empty value that differs from the default.
                    explicitly_default = value is not _MISSING and value == spec.default
                    if not explicitly_default:
                        confirmations.append(
                            Confirmation(
                                path=spec.path,
                                default=spec.default,
                                reason=spec.default_reason or "the documented default",
                                description=spec.description,
                            )
                        )
                # SILENT fields are defaulted without a confirmation.
            continue

        # The field has a value: check it against the allowed set, if any.
        problem = _check_allowed(spec, value)
        if problem:
            errors.append(problem)

    # Cross-field checks that cannot be expressed per-field. The environment
    # must be one of the known values; it is validated here because it is read
    # as an enum elsewhere and a bad value should fail during validation, not
    # at first use.
    env_value = _get_by_path(resolved, "environment")
    if not _is_empty(env_value):
        try:
            Environment.coerce(env_value)
        except Exception as exc:
            errors.append(str(exc))

    result = ValidationResult(
        resolved=resolved, errors=errors, confirmations=confirmations
    )

    _log.info(
        "config validated",
        extra={
            "error_count": len(errors),
            "confirmation_count": len(confirmations),
            "ok": result.ok,
        },
    )
    return result


def load_config(
    source: Union[str, Path, Dict[str, Any]],
    *,
    validate: bool = True,
    require_no_confirmations: bool = False,
) -> ValidationResult:
    """Load a config from a file path, JSON string, or dictionary, then validate.

    Args:
        source: Where to load the config from. One of:
            * a path (``str`` or :class:`pathlib.Path`) to a JSON file,
            * a JSON string, or
            * an already-parsed ``dict``.
        validate: Whether to validate after loading. Almost always ``True``;
            set to ``False`` only to inspect a raw config.
        require_no_confirmations: When ``True`` (the mode for unattended /
            scheduled runs), a config that still has outstanding confirmations
            is treated as invalid, because an unattended run must not rely on
            un-approved defaults. When ``False`` (the mode for the UI), the
            confirmations are returned for the user to approve.

    Returns:
        A :class:`ValidationResult`. When ``validate`` is ``False`` the result
        contains only the loaded config with no errors or confirmations.

    Raises:
        ConfigError: If the source cannot be read or is not valid JSON.
        ValidationError: Never raised here directly; call
            :meth:`ValidationResult.raise_if_invalid` on the result to convert
            hard errors into an exception when you want fail-fast behaviour.
    """
    config = _read_config(source)

    if not validate:
        return ValidationResult(resolved=config)

    result = validate_config(config)

    # For unattended runs, unapproved safe defaults count as blocking, so the
    # run never proceeds on assumptions the user has not seen.
    if require_no_confirmations and result.confirmations:
        for conf in result.confirmations:
            result.errors.append(
                "Unapproved default (not allowed for an unattended run): "
                + conf.message()
            )
        result.confirmations = []

    return result


def _read_config(source: Union[str, Path, Dict[str, Any]]) -> Dict[str, Any]:
    """Read and parse a config from the supported source types.

    Args:
        source: A dict, a path, or a JSON string.

    Returns:
        The parsed config dictionary.

    Raises:
        ConfigError: If the source cannot be read or parsed, or is not a JSON
            object at the top level.
    """
    # Already a dictionary: use it as-is.
    if isinstance(source, dict):
        return source

    # A Path, or a string that names an existing file: read the file.
    if isinstance(source, Path) or (
        isinstance(source, str) and _looks_like_path(source)
    ):
        path = Path(source)
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigError(
                f"Could not read config file: {path}",
                context={"path": str(path)},
                cause=exc,
            ) from exc
    else:
        # Otherwise treat the string as JSON text directly.
        text = str(source)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigError(
            "Config is not valid JSON.",
            context={"line": exc.lineno, "column": exc.colno},
            cause=exc,
        ) from exc

    if not isinstance(parsed, dict):
        raise ConfigError(
            "Config must be a JSON object at the top level, "
            f"got {type(parsed).__name__}."
        )
    return parsed


def _looks_like_path(text: str) -> bool:
    """Heuristically decide whether a string is a file path rather than JSON.

    A JSON config always starts with ``{`` (after optional whitespace). Anything
    that does not, and that points to an existing file, is treated as a path.

    Args:
        text: The string to classify.

    Returns:
        True if the string should be treated as a file path.
    """
    stripped = text.strip()
    if stripped.startswith("{"):
        return False
    # Only treat it as a path if the file actually exists, to avoid mistaking a
    # malformed JSON string for a path.
    try:
        return Path(stripped).exists()
    except OSError:
        return False
