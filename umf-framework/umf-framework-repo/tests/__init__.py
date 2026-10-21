"""Test suite for the Universal Migration & Data Framework.

These tests cover the correctness-critical, pure-Python behaviour of the
framework: config validation, type mapping, chunking, reconciliation, staging
and commit isolation, orchestration, data quality, gold logic, delivery, the
control plane, secrets and the UI. They run without Spark or any external
service, so they can run anywhere, including CI.

Run with pytest (``pytest``) or, without pytest installed, with the bundled
runner (``python tests/run_tests.py``).
"""
