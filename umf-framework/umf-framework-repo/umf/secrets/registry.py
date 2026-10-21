"""Routing secret references to the right resolver.

A :class:`SecretRegistry` holds one resolver per scheme and routes each
reference to the resolver whose scheme matches. It is the single entry point the
rest of the framework uses to turn a reference into a value, and it does one
more essential thing: every value it resolves is registered with the logging
layer so it is scrubbed from any future log message.

Usage pattern:
    1. Build a registry with the resolvers available on this platform.
    2. Call :meth:`SecretRegistry.resolve` with a reference string from the
       config.
    3. Use the returned value immediately to connect; never store it in the
       config, a log, or a long-lived variable.

The registry never caches values. Caching a secret would widen the window in
which it lives in memory for no real benefit; resolving on demand keeps the
exposure minimal.
"""

from __future__ import annotations

from typing import Dict, Iterable, Optional

from umf.core.logging_setup import get_logger, register_secrets
from umf.secrets.base import SecretError, SecretRef, SecretResolver
from umf.secrets.env_resolver import EnvSecretResolver

_log = get_logger("secrets.registry")


class SecretRegistry:
    """Routes secret references to resolvers and protects resolved values.

    Attributes:
        resolvers: A mapping of scheme to the resolver that handles it.
    """

    def __init__(self, resolvers: Optional[Iterable[SecretResolver]] = None) -> None:
        """Create a registry.

        Args:
            resolvers: The resolvers to register. If ``None``, only the portable
                environment-variable resolver is registered, which is the safe
                default that works everywhere. Cloud vault resolvers are added by
                the caller (or by a later part) and passed in here.
        """
        self.resolvers: Dict[str, SecretResolver] = {}
        chosen = list(resolvers) if resolvers is not None else [EnvSecretResolver()]
        for resolver in chosen:
            self.register(resolver)

    def register(self, resolver: SecretResolver) -> None:
        """Add or replace the resolver for a scheme.

        Args:
            resolver: The resolver to register. Its ``scheme`` attribute decides
                which references it handles.

        Raises:
            SecretError: If the resolver does not declare a scheme.
        """
        if not resolver.scheme:
            raise SecretError(
                f"Resolver {type(resolver).__name__} does not declare a scheme."
            )
        self.resolvers[resolver.scheme] = resolver

    def resolve(self, reference: str) -> str:
        """Resolve a reference string to a secret value and protect it.

        The resolved value is registered with the logging layer before it is
        returned, so that even if it later leaks into a message it is scrubbed.

        Args:
            reference: A ``scheme://path`` reference from the config.

        Returns:
            The resolved secret value.

        Raises:
            SecretError: If the reference cannot be parsed, or no resolver is
                registered for its scheme, or resolution fails.
        """
        ref = SecretRef.parse(reference)
        resolver = self.resolvers.get(ref.scheme)
        if resolver is None:
            raise SecretError(
                f"No resolver registered for scheme '{ref.scheme}'. "
                f"Registered schemes: {', '.join(sorted(self.resolvers)) or 'none'}.",
                context={"scheme": ref.scheme},
            )

        value = resolver.resolve(ref)

        # Protect the value from ever appearing in a log line. We deliberately
        # log only the scheme (a non-sensitive identifier), never the value.
        register_secrets([value])
        _log.info("secret resolved", extra={"scheme": ref.scheme})
        return value

    def resolve_optional(self, reference: Optional[str]) -> Optional[str]:
        """Resolve a reference that may be absent.

        Args:
            reference: A reference string, or ``None`` / empty if there is no
                secret to resolve (for example when identity-based auth is used
                and no explicit secret is needed).

        Returns:
            The resolved value, or ``None`` if ``reference`` was empty.
        """
        if not reference:
            return None
        return self.resolve(reference)
