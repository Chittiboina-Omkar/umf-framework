"""Secret handling: references in config, values only in memory at run time.

The config never holds a raw secret, only a reference such as
``env://DB_PASSWORD``. At run time a :class:`~umf.secrets.registry.SecretRegistry`
turns the reference into a value and registers that value with the logging layer
so it is scrubbed from logs.

Public surface:
    SecretRef        a parsed ``scheme://path`` reference
    SecretResolver   the interface each backend implements
    SecretError      raised on a bad reference or failed resolution
    EnvSecretResolver  the portable environment-variable resolver
    SecretRegistry   routes references to resolvers and protects values

Cloud vault resolvers (Databricks, Azure Key Vault, AWS Secrets Manager, GCP
Secret Manager) are added in a later part and register into the same registry.
"""

from umf.secrets.base import SecretError, SecretRef, SecretResolver
from umf.secrets.env_resolver import EnvSecretResolver
from umf.secrets.registry import SecretRegistry

__all__ = [
    "SecretRef",
    "SecretResolver",
    "SecretError",
    "EnvSecretResolver",
    "SecretRegistry",
]
