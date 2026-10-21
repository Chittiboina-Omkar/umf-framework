# Contributing

## Development setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                       # or: python tests/run_tests.py
```

The core has no third-party dependencies, so the tests run without Spark. Keep
it that way: anything that needs Spark or a cloud SDK belongs in an adapter with
a lazy import, so the core stays importable and testable everywhere.

## How to extend the framework (add an adapter)

The whole point of the design is that new capability is a new file behind an
existing interface; the core never changes.

| To add… | Implement the interface in… | Register it in… |
|---------|------------------------------|-----------------|
| a new source database | `umf/adapters/source/` + a type mapper in `umf/typing/mappers/` | the mapper registry (`umf/typing/mappers/__init__.py`) and a reader factory |
| a new cloud storage | `umf/adapters/storage/` (new) behind a storage interface | the storage selector |
| a new file format | `umf/adapters/source/files.py` | the format enum |
| a new table format | `umf/adapters/table_format/` | `get_writer` |
| a new vector store | `umf/adapters/vector/` | `register_vectors` |
| a new DQ rule | `umf/dq/` | the rule type enum + evaluator |
| a new secret vault | `umf/secrets/` | the secret registry |

Rules of thumb:

- Pure logic (planning, decisions, validation, type mapping) must be written so
  it can be unit-tested without Spark. Put the Spark call at the edge.
- Never guess on anything that could lose or corrupt data; fail and surface the
  details instead. Add a config decision if the user needs a way to proceed.
- Never log data values or secrets. Log counts, names and metadata.
- Every new config field goes in `umf/core/config_schema.py` with its risk
  (HIGH = mandatory, SAFE = defaulted with confirmation), so validation and the
  UI pick it up automatically.

## Style

- Google-style docstrings on every public function and class.
- Type hints throughout.
- ASCII only in source; typographic punctuation only in user-facing docs.

## Tests

Add tests for new pure logic in `tests/`. Keep them Spark-free so CI stays fast.
Spark-backed behaviour is validated per platform and recorded in
`docs/CAPABILITY_MATRIX.md`.
