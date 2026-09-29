"""Object descriptor: the immutable, self-certifying trust anchor.

A committed object is described by an :class:`ObjectDescriptor`.  It binds
together:

* the logical object name (an arbitrary caller-chosen byte string),
* the coding parameters (k, m, block_size) and the true object length,
* one stripe-root digest per stripe -- the complete integrity map,
* its own BLAKE2b digest over the canonical binary form.  That digest is
  the *generation*: re-encoding new content under the same name always
  produces a different digest, so an old descriptor physically cannot be
  tricked into authenticating new shares and vice versa.

Every share key, share leaf hash and manifest key lives under the
generation prefix, which is what makes late writes from a previous
generation unable to overwrite a newer committed object.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field

from .coding import STRIPE_MAGIC, CodingParams
from .crypto import DIGEST_SIZE, descriptor_digest
from .errors import DescriptorError

_VERSION = 1
# magic(4) version(2) k(4) m(4) block_size(8) length(8) name_len(2) roots(32*S)
_HEADER = struct.Struct(">4sHIIQQH")


@dataclass(frozen=True)
class ObjectDescriptor:
    """Immutable description of one committed object generation."""

    name: bytes
    params: CodingParams
    length: int
    stripe_roots: tuple[bytes, ...]
    digest: bytes = field(repr=False)

    @property
    def k(self) -> int:
        return self.params.k

    @property
    def m(self) -> int:
        return self.params.m

    @property
    def n(self) -> int:
        return self.params.n

    @property
    def block_size(self) -> int:
        return self.params.block_size

    @property
    def stripe_count(self) -> int:
        return len(self.stripe_roots)

    @property
    def generation(self) -> str:
        return self.digest.hex()

    def to_bytes(self) -> bytes:
        """Canonical, deterministic binary serialization."""
        if len(self.name) > 0xFFFF:
            raise DescriptorError("object name too long (max 65535 bytes)")
        header = _HEADER.pack(
            STRIPE_MAGIC, _VERSION, self.params.k, self.params.m,
            self.params.block_size, self.length, len(self.name),
        )
        body = header + self.name + b"".join(self.stripe_roots)
        # The digest is transported separately in a fixed-size trailer so a
        # descriptor is one self-contained blob on storage.
        return body + self.digest

    def to_public_dict(self) -> dict:
        """JSON-friendly view suitable for persisting by callers."""
        return {
            "name": self.name.decode("utf-8", "surrogateescape"),
            "generation": self.generation,
            "k": self.k,
            "m": self.m,
            "n": self.n,
            "block_size": self.block_size,
            "length": self.length,
            "stripe_count": self.stripe_count,
            "stripe_roots": [r.hex() for r in self.stripe_roots],
        }

    def verify(self) -> None:
        """Recompute and check the self-digest and internal consistency."""
        canonical = self._canonical()
        if descriptor_digest(canonical) != self.digest:
            raise DescriptorError("descriptor digest mismatch")
        if len(self.stripe_roots) != self.params.stripe_count(self.length):
            raise DescriptorError("stripe root count does not match length")
        for root in self.stripe_roots:
            if len(root) != DIGEST_SIZE:
                raise DescriptorError("stripe roots must be 32 bytes")

    def _canonical(self) -> bytes:
        return _HEADER.pack(
            STRIPE_MAGIC, _VERSION, self.params.k, self.params.m,
            self.params.block_size, self.length, len(self.name),
        ) + self.name + b"".join(self.stripe_roots)


def build_descriptor(name: bytes, params: CodingParams, length: int,
                     stripe_roots: list[bytes]) -> ObjectDescriptor:
    """Construct a descriptor and compute its digest/generation."""
    params.validate()
    if len(name) == 0:
        raise DescriptorError("object name must not be empty")
    if len(name) > 0xFFFF:
        raise DescriptorError("object name too long (max 65535 bytes)")
    expected = params.stripe_count(length)
    if len(stripe_roots) != expected:
        raise DescriptorError(
            f"expected {expected} stripe roots for length {length}, "
            f"got {len(stripe_roots)}")
    for root in stripe_roots:
        if len(root) != DIGEST_SIZE:
            raise DescriptorError("stripe roots must be 32 bytes")
    desc = ObjectDescriptor(
        name=name, params=params, length=length,
        stripe_roots=tuple(stripe_roots), digest=b"\x00" * DIGEST_SIZE)
    digest = descriptor_digest(desc._canonical())
    return ObjectDescriptor(
        name=name, params=params, length=length,
        stripe_roots=tuple(stripe_roots), digest=digest)


def parse_descriptor(blob: bytes) -> ObjectDescriptor:
    """Parse, validate and authenticate a serialized descriptor."""
    if len(blob) < _HEADER.size + DIGEST_SIZE:
        raise DescriptorError("descriptor blob too short")
    magic, version, k, m, block_size, length, name_len = _HEADER.unpack_from(blob)
    if magic != STRIPE_MAGIC:
        raise DescriptorError("bad descriptor magic")
    if version != _VERSION:
        raise DescriptorError(f"unsupported descriptor version {version}")
    name_start = _HEADER.size
    name_end = name_start + name_len
    roots_end = len(blob) - DIGEST_SIZE
    if name_end > roots_end:
        raise DescriptorError("descriptor truncated in name field")
    roots_blob = blob[name_end:roots_end]
    digest = blob[roots_end:]
    if len(roots_blob) % DIGEST_SIZE != 0:
        raise DescriptorError("stripe roots are not 32-byte aligned")
    name = blob[name_start:name_end]
    roots = [roots_blob[i:i + DIGEST_SIZE]
             for i in range(0, len(roots_blob), DIGEST_SIZE)]
    try:
        params = CodingParams(k=k, m=m, block_size=block_size)
        params.validate()
    except Exception as exc:  # noqa: BLE001 - normalize to DescriptorError
        raise DescriptorError(f"invalid coding parameters: {exc}") from exc
    desc = ObjectDescriptor(
        name=name, params=params, length=length,
        stripe_roots=tuple(roots), digest=digest)
    desc.verify()
    return desc
