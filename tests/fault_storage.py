"""Test support: fault-injecting storage and blob/key manipulation helpers.

The kernel deliberately has no "delete one share of an object" API, so
tests exercise corruption and loss at the storage layer, which is exactly
where those faults occur in production.
"""

from __future__ import annotations

import asyncio
import secrets

from stripe_recovery_core.crypto import content_digest
from stripe_recovery_core.storage import keys
from stripe_recovery_core.errors import NotFound


class FaultStorage:
    """Wraps a storage backend with scriptable faults.

    * ``delete_blobs(predicate)`` / ``corrupt_blob`` -- physical damage,
    * ``fail_matching(substring)`` -- make gets/puts raise IOError,
    * ``slow_blobs`` -- delay delivery (backpressure tests),
    * records every operation for assertions.
    """

    def __init__(self, inner) -> None:
        self.inner = inner
        self.get_fail: set[str] = set()
        self.put_fail: set[str] = set()
        self.cas_fail: set[str] = set()
        self.slow: dict[str, float] = {}
        self.events: list[tuple[str, str]] = []
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> bytes:
        self.events.append(("get", key))
        for sub in self.get_fail:
            if sub in key:
                raise OSError(f"injected get failure for {sub}")
        if key in self.slow:
            await asyncio.sleep(self.slow[key])
        return await self.inner.get(key)

    async def put(self, key: str, value: bytes) -> None:
        self.events.append(("put", key))
        for sub in self.put_fail:
            if sub in key:
                raise OSError(f"injected put failure for {sub}")
        return await self.inner.put(key, value)

    async def put_if_absent(self, key: str, value: bytes) -> bool:
        self.events.append(("cas", key))
        for sub in self.cas_fail:
            if sub in key:
                raise OSError(f"injected cas failure for {sub}")
        return await self.inner.put_if_absent(key, value)

    async def delete(self, key: str) -> None:
        self.events.append(("delete", key))
        return await self.inner.delete(key)

    # -- physical damage helpers ----------------------------------------

    async def list_keys(self) -> list[str]:
        data = self.inner._data
        return sorted(data.keys())

    async def blob_keys(self) -> list[str]:
        return [k for k in await self.list_keys() if k.startswith("blob/")]

    async def delete_blob_for(self, block: bytes) -> None:
        await self.inner.delete(keys.blob_key(block))

    async def delete_share(self, desc, stripe: int, share: int,
                           *, leaves_for) -> None:
        """Delete the physical blob backing manifest position (stripe,share)."""
        digest_value = leaves_for(stripe)[share]
        await self.inner.delete(keys.blob_key_for_digest(digest_value))

    async def corrupt_share_blob(self, desc, stripe: int, share: int,
                                 *, leaves_for) -> tuple[str, bytes]:
        """Flip one bit in the physical blob for a manifest position.

        The manifest still names the *old* digest, while storage now holds
        bytes with a different digest: models exactly "one byte of one
        share changed on disk".
        """
        digest_value = leaves_for(stripe)[share]
        bkey = keys.blob_key_for_digest(digest_value)
        original = await self.inner.get(bkey)
        mutated = bytearray(original)
        mutated[0] ^= 0x01
        await self.inner.put(bkey, bytes(mutated))
        return bkey, original

    async def alias_blob(self, digest_value: bytes, payload: bytes) -> str:
        """Write attacker/legacy bytes at a chosen content key."""
        bkey = keys.blob_key_for_digest(digest_value)
        await self.inner.put(bkey, payload)
        return bkey

    async def make_share_key(self, desc, stripe: int, share: int) -> str:
        # Provided for readability in tests.
        return keys.manifest_key(desc, stripe)


def flip_bit(data: bytes, index: int = 0) -> bytes:
    out = bytearray(data)
    out[index] ^= 0x01
    return bytes(out)


def leaves_of(desc_blob_for_stripe):  # pragma: no cover - documentation helper
    raise NotImplementedError
