"""A secret resolver that reads from environment variables.

Scheme: ``env``. A reference ``env://DB_PASSWORD`` resolves to the value of the
``DB_PASSWORD`` environment variable.

This resolver depends only on the standard library, so it works on every
platform and is the natural choice for local development and tests. In
production the cloud vault resolvers (added later) are usually preferred, but
environment variables remain a valid, portable option, for example when a
platform injects a secret into the job environment.
"""

from __future__ import annotations

import os

from umf.secrets.base import SecretError, SecretRef, SecretResolver


class EnvSecretResolver(SecretResolver):
    """Resolve secrets from environment variables.

    The reference path is the environment variable name. The value is read only
    when needed and is never logged.
    """

    scheme = "env"

    def resolve(self, ref: SecretRef) -> str:
        """Return the value of the environment variable named by the reference.

        Args:
            ref: A reference whose path is the environment variable name.

        Returns:
            The environment variable's value.

        Raises:
            SecretError: If the environment variable is not set. The error
                message names the variable (a non-sensitive identifier) but
                never a value.
        """
        name = ref.path
        value = os.environ.get(name)
        if value is None:
            raise SecretError(
                f"Environment variable '{name}' is not set, "
                "so the secret reference cannot be resolved.",
                context={"scheme": ref.scheme, "variable": name},
            )
        return value
