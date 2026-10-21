"""The pure model behind the notebook UI.

The UI is a convenience for building and running a config; it is not where any
logic lives. Everything the UI needs to decide is here, as pure functions that
can be tested without a notebook or ipywidgets:

    * group the config fields into the sections a user fills in;
    * decide which sections are relevant for the chosen steps, so the form only
      shows what matters;
    * turn the flat field values the form collects into the nested config the
      framework expects; and
    * turn a config back into flat field values, so an existing config can be
      loaded into the form.

Because the field definitions come from the one config schema (Part 1), the UI
always reflects exactly the fields the framework validates, and the mandatory
and safe-with-confirmation distinctions carry straight through.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence

from umf.core.config_schema import FieldSpec, schema
from umf.core.constants import FieldRisk, Step


# The section each config area belongs to, keyed by the leading part of the
# field path. A path like "curated.mode" is in the "Curated" section.
_SECTION_BY_PREFIX: Dict[str, str] = {
    "environment": "General",
    "client_id": "General",
    "platform": "General",
    "steps": "General",
    "source": "Source",
    "load": "Load",
    "raw": "Raw",
    "schema": "Schema",
    "curated": "Curated",
    "target": "Target",
    "recon": "Reconciliation",
    "delivery": "Delivery",
    "gold": "Gold",
    "operation": "Operation",
}

# Which sections are relevant for each step, so the form hides sections that do
# not apply to the chosen pipeline.
_SECTIONS_BY_STEP: Dict[Step, List[str]] = {
    Step.DWH_TO_FILES: ["General", "Source", "Load", "Delivery", "Operation"],
    Step.DWH_TO_TABLE: ["General", "Source", "Load", "Schema", "Raw", "Target", "Reconciliation", "Operation"],
    Step.FILES_TO_RAW: ["General", "Source", "Schema", "Raw", "Target", "Reconciliation", "Operation"],
    Step.RAW_TO_CURATED: ["General", "Curated", "Target", "Reconciliation", "Operation"],
    Step.CURATED_TO_GOLD: ["General", "Gold", "Delivery", "Target", "Reconciliation", "Operation"],
    Step.VOLUME_TABLE: ["General", "Operation"],
    Step.VECTORS: ["General", "Operation"],
}


@dataclass
class UiField:
    """A field as the UI presents it.

    Attributes:
        path: The config path (for example ``"curated.mode"``).
        section: The section it belongs to.
        label: A short label derived from the path.
        mandatory: Whether it must be filled (a high-risk field).
        default: The default applied when left empty (for safe fields).
        allowed: The allowed values, if any (for a dropdown).
        description: The field's help text.
        example: An example value.
    """

    path: str
    section: str
    label: str
    mandatory: bool
    default: Any
    allowed: Any
    description: str
    example: Any


def _section_of(path: str) -> str:
    """Return the UI section for a config path.

    Args:
        path: The config path.

    Returns:
        The section name, defaulting to ``"Other"`` for unmapped prefixes.
    """
    prefix = path.split(".", 1)[0]
    return _SECTION_BY_PREFIX.get(prefix, "Other")


def _label_of(path: str) -> str:
    """Derive a human label from a config path.

    Args:
        path: The config path.

    Returns:
        A readable label, for example ``"mode"`` from ``"curated.mode"``.
    """
    return path.split(".")[-1].replace("_", " ")


def ui_fields() -> List[UiField]:
    """Return every config field as a UI field, grouped and labelled.

    Returns:
        The list of :class:`UiField`, in the schema's order.
    """
    out: List[UiField] = []
    for spec in schema():
        allowed = spec.allowed
        allowed_values = None
        if allowed is not None:
            allowed_values = (
                allowed.values() if hasattr(allowed, "values") else list(allowed)
            )
        out.append(
            UiField(
                path=spec.path,
                section=_section_of(spec.path),
                label=_label_of(spec.path),
                mandatory=spec.risk == FieldRisk.HIGH,
                default=spec.default,
                allowed=allowed_values,
                description=spec.description,
                example=spec.example,
            )
        )
    return out


def relevant_sections(steps: Sequence[Step]) -> List[str]:
    """Return the sections to show for the chosen steps, in a stable order.

    Args:
        steps: The steps the user has chosen.

    Returns:
        The de-duplicated list of sections relevant to any of the steps, always
        starting with ``"General"``.
    """
    order = ["General", "Source", "Load", "Schema", "Raw", "Curated", "Gold",
             "Target", "Delivery", "Reconciliation", "Operation", "Other"]
    wanted = {"General"}
    for step in steps:
        wanted.update(_SECTIONS_BY_STEP.get(step, []))
    return [s for s in order if s in wanted]


def build_config(values: Dict[str, Any]) -> Dict[str, Any]:
    """Turn flat ``path -> value`` form values into a nested config.

    Empty values are dropped, so the framework's validation applies its defaults
    and confirmations rather than the UI inventing empty entries.

    Args:
        values: A mapping of config path to the value the form collected.

    Returns:
        The nested config dictionary.
    """
    config: Dict[str, Any] = {}
    for path, value in values.items():
        if value is None or (isinstance(value, str) and value.strip() == ""):
            continue
        _set_path(config, path, value)
    return config


def flatten_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a nested config into ``path -> value`` for loading into the form.

    Only the paths the schema knows about are returned, so loading an existing
    config populates exactly the form's fields.

    Args:
        config: The nested config.

    Returns:
        A mapping of known config path to its value.
    """
    flat: Dict[str, Any] = {}
    for spec in schema():
        value = _get_path(config, spec.path)
        if value is not None:
            flat[spec.path] = value
    return flat


def _set_path(config: Dict[str, Any], path: str, value: Any) -> None:
    """Set a value into a nested dict by dotted path, creating parents."""
    parts = path.split(".")
    node = config
    for part in parts[:-1]:
        existing = node.get(part)
        if not isinstance(existing, dict):
            existing = {}
            node[part] = existing
        node = existing
    node[parts[-1]] = value


def _get_path(config: Dict[str, Any], path: str) -> Any:
    """Read a value from a nested dict by dotted path, or ``None`` if absent."""
    node: Any = config
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node
