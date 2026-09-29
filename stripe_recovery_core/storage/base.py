"""Async storage protocol and key layout.

Callers supply their own storage implementation (S3, GCS, a database,
memory, ...).  The kernel only requires four async operations and never
assumes any particular cloud account.

Key layout (every data key is generation-scoped):

* ``head/<name-key>``               -> serialized descriptor of the commit
* ``gen/<generation>/desc``        -> serialized descriptor blob
* ``gen/<generation>/s/<i>/manifest``
* ``gen/<generation>/s/<i>/h/<j>`` -- coded share block of stripe i, share j

Because a repair writer can only touch keys under *its* descriptor's
generation prefix, a late repair of an old generation physically cannot
replace any key of a newer committed object.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class AsyncStorage(Protocol):
    """Minimal asynchronous byte-keyed storage contract."""

    async def get(self, key: str) -> bytes:
        """Return the object at *key*.

        Must raise :class:`stripe_recovery_core.errors.NotFound` when the
        key does not exist; other failures should be raised (optionally
        wrapped in :class:`StorageError`).
        """
        ...

    async def put(self, key: str, value: bytes) -> None:
        """Unconditionally store *value* at *key*."""
        ...

    async def put_if_absent(self, key: str, value: bytes) -> bool:
        """Store only if *key* is absent.

        Returns True when this call created the key.  Implementations that
        natively lack compare-and-swap must still emulate the atomic
        guarantee; the repair safety argument depends on it.
        """
        ...

    async def delete(self, key: str) -> None:
        """Delete *key*; must succeed (not raise) when it is already absent."""
        ...
