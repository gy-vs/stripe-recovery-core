"""Cryptographic binding primitives.

The trust chain (bottom-up), deliberately arranged so streaming writers
can authenticate every stripe the moment it is encoded, before the whole
object length is known:

1. *content digest* -- plain BLAKE2b(share block).  Share blobs are
   content-addressed on storage at ``blob/<digest>``.
2. *stripe root* -- keyed hash over the *ordered* list of a stripe's n
   content digests (and the stripe index).  It authenticates a stripe
   manifest: a block fetched at share position j is only authentic when
   its content digest equals manifest entry j.  This is where mix-in
   attacks die -- a share from another encoding of the same object name
   has a different content digest and fails at this exact position.
3. *descriptor digest* -- keyed hash over the canonical descriptor, which
   contains every stripe root plus object name, parameters and true
   length.  That digest is the generation identifier.  Tail padding is
   authenticated indirectly: reconstructed padding blocks are verified to
   be zero before bytes are returned, and the true length lives only in
   the descriptor.

Content-addressed shares may be shared between generations that contain
identical blocks; that cannot change any object's meaning, since meaning
is fixed by the descriptor's ordered root list.
"""

from __future__ import annotations

import hashlib

DIGEST_SIZE = 32

_DOMAIN_STRIPE_ROOT = b"src/v1/stripe-root"
_DOMAIN_MANIFEST = b"src/v1/manifest"
_DOMAIN_DESCRIPTOR = b"src/v1/descriptor"
_DOMAIN_HEAD = b"src/v1/head"
_DOMAIN_NAME = b"src/v1/name"


def content_digest(block: bytes) -> bytes:
    """Unguarded BLAKE2b content hash of one share block."""
    return hashlib.blake2b(block, digest_size=DIGEST_SIZE).digest()


def stripe_root(stripe_index: int, leaves: list[bytes]) -> bytes:
    """Root over a stripe's ordered n content digests; stored on descriptor."""
    h = hashlib.blake2b(digest_size=DIGEST_SIZE, key=_DOMAIN_STRIPE_ROOT)
    h.update(stripe_index.to_bytes(8, "big"))
    h.update(len(leaves).to_bytes(4, "big"))
    if not leaves:
        raise ValueError("empty stripe")
    for leaf in leaves:
        if len(leaf) != DIGEST_SIZE:
            raise ValueError("leaf digests must be 32 bytes")
        h.update(leaf)
    return h.digest()


def manifest_blob(leaves: list[bytes]) -> bytes:
    """Serialize a stripe manifest (n concatenated content digests)."""
    if not leaves:
        raise ValueError("empty manifest")
    for leaf in leaves:
        if len(leaf) != DIGEST_SIZE:
            raise ValueError("leaf digests must be 32 bytes")
    return _DOMAIN_MANIFEST + len(leaves).to_bytes(4, "big") + b"".join(leaves)


def parse_manifest(blob: bytes, n: int) -> list[bytes]:
    """Parse and validate a stripe manifest blob for an n-share layout."""
    prefix = _DOMAIN_MANIFEST + n.to_bytes(4, "big")
    if len(blob) != len(prefix) + n * DIGEST_SIZE or not blob.startswith(prefix):
        raise ValueError("malformed stripe manifest")
    off = len(prefix)
    return [blob[off + i * DIGEST_SIZE: off + (i + 1) * DIGEST_SIZE]
            for i in range(n)]


def descriptor_digest(canonical: bytes) -> bytes:
    """Self-digest of the canonical binary descriptor (also generation id)."""
    h = hashlib.blake2b(digest_size=DIGEST_SIZE, key=_DOMAIN_DESCRIPTOR)
    h.update(canonical)
    return h.digest()


def head_key(name: bytes) -> str:
    """Storage key for the latest committed descriptor of an object name."""
    h = hashlib.blake2b(digest_size=DIGEST_SIZE, key=_DOMAIN_HEAD)
    h.update(_DOMAIN_NAME)
    h.update(len(name).to_bytes(4, "big"))
    h.update(name)
    return "head/" + h.hexdigest()


def generation_hex(desc_digest: bytes) -> str:
    """Filesystem-safe generation identifier."""
    return desc_digest.hex()
