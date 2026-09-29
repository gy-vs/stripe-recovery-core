"""Test helpers: read descriptors/manifests back from storage."""

from __future__ import annotations

from stripe_recovery_core.crypto import parse_manifest
from stripe_recovery_core.descriptor import parse_descriptor
from stripe_recovery_core.storage import keys


async def stored_descriptor(storage, desc):
    raw = await storage.get(keys.descriptor_key(desc))
    return parse_descriptor(raw)


async def head_descriptor(storage, name: bytes):
    raw = await storage.get(keys.head_key(name))
    return parse_descriptor(raw)


async def manifest_leaves(storage, desc, stripe: int):
    raw = await storage.get(keys.manifest_key(desc, stripe))
    return parse_manifest(raw, desc.n)


def count_keys(storage, *, prefix: str = "") -> int:
    return sum(1 for k in storage._data if k.startswith(prefix))
