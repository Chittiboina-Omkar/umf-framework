"""The vector store interface.

Vectors are written to a store that varies by client (Databricks Vector Search,
Azure AI Search, OpenSearch, pgvector, and others). This interface is the single
contract those backends implement, so the vectors step does not depend on any
one of them. A concrete store is selected by name and added as an adapter.

The interface is intentionally small: list the ids currently stored (so the
step can compute the delete-sync), upsert vectors with their metadata, and
delete vectors by id. That is enough to keep a store faithful to its source.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence


class VectorStore:
    """Interface every vector store backend implements.

    Attributes:
        name: The store's name, for diagnostics.
    """

    name: str = ""

    def list_ids(self, collection: str) -> List[str]:
        """Return the ids currently stored in a collection.

        Used to compute which vectors to add or delete so the store stays in
        step with the source.

        Args:
            collection: The collection / index name.

        Returns:
            The list of stored ids.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def upsert(
        self, collection: str, items: Sequence[Dict[str, Any]]
    ) -> int:
        """Insert or update vectors with their metadata.

        Args:
            collection: The collection / index name.
            items: The vectors to write. Each item has at least an ``id``, a
                ``vector`` (the embedding) and a ``metadata`` mapping.

        Returns:
            The number of vectors written.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError

    def delete(self, collection: str, ids: Iterable[str]) -> int:
        """Delete vectors by id.

        Args:
            collection: The collection / index name.
            ids: The ids to delete.

        Returns:
            The number of vectors deleted.

        Raises:
            NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError
