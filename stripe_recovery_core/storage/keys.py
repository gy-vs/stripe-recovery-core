"""Storage key layout.

* ``head/<name-hash>``                  -> serialized descriptor (latest)
* ``gen/<generation>/desc``             -> serialized descriptor blob
* ``gen/<generation>/s/<i>/manifest``   -> stripe manifest (generation scoped)
* ``blob/<content-digest>``             -> one coded share block (content
  addressed; may be shared by generations that contain identical blocks)

A repair of an old generation can only create ``blob`` keys for content
that generation references and CAS-write keys under its *own*
``gen/<old>/`` prefix.  It physically cannot overwrite a newer
generation's descriptors or manifests.
"""

from __future__ import annotations

from ..crypto import head_key as _head_key
from ..descriptor import ObjectDescriptor

__all__ = ["head_key", "descriptor_key", "manifest_key", "blob_key",
           "generation_prefix"]


def head_key(name: bytes) -> str:
    return _head_key(name)


def descriptor_key(descriptor: ObjectDescriptor) -> str:
    return f"gen/{descriptor.generation}/desc"


def manifest_key(descriptor: ObjectDescriptor, stripe_index: int) -> str:
    return f"gen/{descriptor.generation}/s/{stripe_index}/manifest"


def blob_key(content: bytes) -> str:
    from ..crypto import content_digest
    return "blob/" + content_digest(bytes(content)).hex()


def blob_key_for_digest(content_digest_value: bytes) -> str:
    return "blob/" + content_digest_value.hex()


def generation_prefix(descriptor: ObjectDescriptor) -> str:
    """Prefix of every generation-specific key (for cleanup)."""
    return f"gen/{descriptor.generation}/"
