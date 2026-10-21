"""Adapters: the plug-in layer that makes the framework multi-cloud.

Everything platform- or vendor-specific lives here, behind small interfaces, so
the framework core never changes when a new cloud, storage system, catalog,
source or table format is added. Part 3 introduces:

    spark_session   obtaining a Spark session on any platform (layer L0)
    source/         source readers (layer L1), starting with files

Storage, catalog, table-format and vector adapters are added in later parts,
each behind its own interface in this package.
"""
