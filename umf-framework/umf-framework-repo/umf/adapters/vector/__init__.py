"""Vector store adapters.

The vectors step writes through the :class:`~umf.adapters.vector.base.VectorStore`
interface, so the store backend (Databricks Vector Search, Azure AI Search,
OpenSearch, pgvector, and others) is a plug-in. Concrete backends are added here
in a later part; each implements the same small contract (list ids, upsert,
delete) that keeps a store faithful to its source.
"""

from umf.adapters.vector.base import VectorStore

__all__ = ["VectorStore"]
