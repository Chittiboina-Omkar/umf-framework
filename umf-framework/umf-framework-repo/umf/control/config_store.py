"""Versioning, audit and rollback for configs that live at the client.

Because the config lives in the client environment (not in our repository), we
cannot rely on Git history and pull-request review for versioning and audit.
This module provides the equivalent instead:

    * Every saved config becomes a new, immutable version with a monotonic
      number. Previous versions are never changed or deleted, so the full
      history is always available.
    * Each version records who saved it, when, an optional note, and a SHA-256
      hash of its canonical content, so tampering is detectable and two
      identical saves are recognisable.
    * Rollback does not erase history; it writes the old content as a new
      version, so the timeline stays append-only and auditable.

This gives you reproducibility (every run records the exact config version it
used) and a clear change trail, entirely within the client's own storage.

The interface is :class:`ConfigStore`; the portable default,
:class:`FileConfigStore`, stores versions as files in a directory. A table-backed
store writing to the ``config_versions`` control table is added later and
implements the same interface.
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ConfigVersion:
    """Metadata describing one saved version of a config.

    Attributes:
        version: The monotonic version number (starts at 1).
        client_id: The client the config belongs to.
        environment: The environment the config applies to.
        config_hash: SHA-256 of the canonical config JSON.
        author: Who saved the version.
        note: An optional change note.
        created_at: When the version was saved, in UTC ISO format.
    """

    version: int
    client_id: str
    environment: str
    config_hash: str
    author: str
    note: str
    created_at: str

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serialisable view of the metadata.

        Returns:
            The metadata as a plain dictionary.
        """
        return {
            "version": self.version,
            "client_id": self.client_id,
            "environment": self.environment,
            "config_hash": self.config_hash,
            "author": self.author,
            "note": self.note,
            "created_at": self.created_at,
        }


def config_hash(config: Dict[str, Any]) -> str:
    """Compute a stable SHA-256 hash of a config.

    The config is serialised with sorted keys and no insignificant whitespace,
    so logically identical configs hash identically regardless of key order or
    formatting.

    Args:
        config: The config dictionary.

    Returns:
        The hex SHA-256 digest of the canonical form.
    """
    canonical = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class ConfigStore:
    """Interface for versioned, audited config storage."""

    def save(
        self, config: Dict[str, Any], *, author: str, note: str = ""
    ) -> ConfigVersion:
        """Save a config as a new version and return its metadata.

        Saving a config identical to the current latest version is allowed and
        still produces a new version row, so the audit trail reflects that
        someone re-saved it; implementations may choose to short-circuit, but
        must never silently drop the audit record.

        Args:
            config: The config to save.
            author: Who is saving it.
            note: An optional change note.

        Returns:
            The :class:`ConfigVersion` for the new version.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def get(self, version: Optional[int] = None) -> Dict[str, Any]:
        """Return the config content for a version.

        Args:
            version: The version to read. If ``None``, the latest version is
                returned.

        Returns:
            The config dictionary for that version.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def history(self) -> List[ConfigVersion]:
        """Return the metadata of every version, newest last.

        Returns:
            The list of :class:`ConfigVersion` entries.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def rollback(self, to_version: int, *, author: str) -> ConfigVersion:
        """Roll back by writing an old version's content as a new version.

        Args:
            to_version: The version whose content to restore.
            author: Who is performing the rollback.

        Returns:
            The :class:`ConfigVersion` of the newly written version.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError


class FileConfigStore(ConfigStore):
    """A config store backed by a directory of version files.

    Layout under ``root``:
        ``index.json``          the ordered list of version metadata
        ``config_v<N>.json``    the content of version N

    The index and content files are written atomically (temp file then rename)
    so a crash cannot corrupt the history.

    Attributes:
        root: The directory holding the index and version files.
        client_id: The client these configs belong to.
        environment: The environment these configs apply to.
    """

    def __init__(self, root: str, *, client_id: str, environment: str) -> None:
        """Create the store, ensuring its directory exists.

        Args:
            root: Directory to store versions in.
            client_id: The client identifier, recorded on every version.
            environment: The environment, recorded on every version.
        """
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.client_id = client_id
        self.environment = environment
        self._lock = threading.Lock()
        self._index_path = self.root / "index.json"

    def _read_index(self) -> List[Dict[str, Any]]:
        """Read the version index, returning an empty list if absent."""
        if not self._index_path.exists():
            return []
        try:
            return json.loads(self._index_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []

    def _write_atomic(self, path: Path, data: Any) -> None:
        """Write JSON data to a path atomically.

        Args:
            path: The destination file.
            data: The JSON-serialisable data to write.
        """
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(path)

    def _version_path(self, version: int) -> Path:
        """Return the content file path for a version.

        Args:
            version: The version number.

        Returns:
            The path of that version's content file.
        """
        return self.root / f"config_v{version}.json"

    def save(
        self, config: Dict[str, Any], *, author: str, note: str = ""
    ) -> ConfigVersion:
        """Save a new config version. See :meth:`ConfigStore.save`.

        Args:
            config: The config to save.
            author: Who is saving it.
            note: An optional change note.

        Returns:
            The new :class:`ConfigVersion`.
        """
        with self._lock:
            index = self._read_index()
            next_version = (index[-1]["version"] + 1) if index else 1
            meta = ConfigVersion(
                version=next_version,
                client_id=self.client_id,
                environment=self.environment,
                config_hash=config_hash(config),
                author=author,
                note=note,
                created_at=_now(),
            )
            # Write the content first; if indexing failed afterwards, the orphan
            # content file is harmless and the save simply did not take effect.
            self._write_atomic(self._version_path(next_version), config)
            index.append(meta.to_dict())
            self._write_atomic(self._index_path, index)
            return meta

    def get(self, version: Optional[int] = None) -> Dict[str, Any]:
        """Return a version's config content. See :meth:`ConfigStore.get`.

        Args:
            version: The version to read, or ``None`` for the latest.

        Returns:
            The config dictionary.

        Raises:
            FileNotFoundError: If there are no versions, or the requested
                version does not exist.
        """
        with self._lock:
            index = self._read_index()
            if not index:
                raise FileNotFoundError("No config versions have been saved yet.")
            resolved = index[-1]["version"] if version is None else version
            path = self._version_path(resolved)
            if not path.exists():
                raise FileNotFoundError(f"Config version {resolved} not found.")
            return json.loads(path.read_text(encoding="utf-8"))

    def history(self) -> List[ConfigVersion]:
        """Return every version's metadata. See :meth:`ConfigStore.history`.

        Returns:
            The list of :class:`ConfigVersion` entries, oldest first.
        """
        with self._lock:
            return [
                ConfigVersion(
                    version=row["version"],
                    client_id=row["client_id"],
                    environment=row["environment"],
                    config_hash=row["config_hash"],
                    author=row["author"],
                    note=row.get("note", ""),
                    created_at=row["created_at"],
                )
                for row in self._read_index()
            ]

    def rollback(self, to_version: int, *, author: str) -> ConfigVersion:
        """Roll back by re-saving an old version's content as a new version.

        This keeps the history append-only: nothing is deleted, and the new
        version's note records which version it restored.

        Args:
            to_version: The version to restore.
            author: Who is performing the rollback.

        Returns:
            The new :class:`ConfigVersion`.
        """
        content = self.get(to_version)
        return self.save(
            content, author=author, note=f"rollback to version {to_version}"
        )


def _now() -> str:
    """Return the current UTC time as an ISO 8601 string.

    Returns:
        The current UTC time.
    """
    return datetime.now(timezone.utc).isoformat()
