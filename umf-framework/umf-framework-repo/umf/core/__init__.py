"""Core foundation of the framework.

This sub-package holds the platform independent building blocks that every
other part of the framework relies on:

    constants        Enumerations shared across the whole framework.
    exceptions       The exception hierarchy, with error categories.
    platform_detect  Best-effort detection of the runtime platform.
    masking          Redaction of secrets before anything is logged.
    logging_setup    Crash-safe, structured logging.
    config_schema    Declarative description of every config field.
    config_loader    Loading, resolving and validating a JSON config.
    run_context      The object that tracks one run from start to finish.

Nothing in this sub-package imports PySpark or any cloud SDK, so it can be
imported and unit-tested anywhere, including a plain laptop.
"""
