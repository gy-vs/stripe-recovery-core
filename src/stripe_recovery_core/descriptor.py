"""Object descriptor: the persistable handle that binds content to shards.

A descriptor records, for one committed generation of an object:

* the original length and the encoding parameters (k, m, stripe/shard sizes);
* where the shards live (storage prefix + name + generation determine every
  piece key deterministically);
* the integrity basis: SHA-256 of the whole content, SHA-256 of every stripe,
  and SHA-256 of every encoded shard piece, all folded into ``manifest_root``.

Descriptors are self-contained and content-addressing: any byte that does not
match these hashes is rejected by the kernel, so a descriptor can never be
satisfied by content from a different write, and a tampered descriptor fails
closed instead of silently describing different bytes.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

from .errors import DescriptorError

FORMAT_VERSION = "stripe-recovery-core/1"


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def compute_manifest_root(stripes: list["StripeManifest"] | tuple["StripeManifest", ...]) -> str:
    h = hashlib.sha256()
    for s in stripes:
        h.update(bytes.fromhex(s.sha256))
        for ph in s.piece_hashes:
            h.update(bytes.fromhex(ph))
    return h.hexdigest()


@dataclass(frozen=True)
class StripeManifest:
    """Integrity record for one stripe of plaintext and its m encoded pieces."""

    index: int
    length: int  # real plaintext bytes in this stripe (<= stripe_size)
    sha256: str  # hash of the real (unpadded) plaintext of this stripe
    piece_hashes: tuple[str, ...]  # one SHA-256 per encoded shard piece, len == m

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "length": self.length,
            "sha256": self.sha256,
            "pieces": list(self.piece_hashes),
        }

    @staticmethod
    def from_dict(d: dict) -> "StripeManifest":
        try:
            return StripeManifest(
                index=int(d["index"]),
                length=int(d["length"]),
                sha256=str(d["sha256"]),
                piece_hashes=tuple(str(p) for p in d["pieces"]),
            )
        except (KeyError, TypeError, ValueError) as e:
            raise DescriptorError(f"malformed stripe manifest: {e}") from e


@dataclass(frozen=True)
class ObjectDescriptor:
    """Committed, persistable description of one generation of an object."""

    name: str
    generation: str
    length: int
    k: int
    m: int
    stripe_size: int
    shard_size: int
    codec: str
    storage_prefix: str
    content_sha256: str
    manifest_root: str
    created_at: str
    stripes: tuple[StripeManifest, ...] = field(default_factory=tuple)
    writer: str | None = None
    format: str = FORMAT_VERSION

    # -- derived views -----------------------------------------------------

    @property
    def stripe_count(self) -> int:
        return len(self.stripes)

    def stripe_indices_for(self, offset: int, length: int) -> range:
        """Stripe indices covering the byte range [offset, offset+length)."""
        if length <= 0:
            return range(0)
        first = offset // self.stripe_size
        last = (offset + length - 1) // self.stripe_size
        return range(first, last + 1)

    # -- serialisation -----------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "format": self.format,
            "name": self.name,
            "generation": self.generation,
            "length": self.length,
            "k": self.k,
            "m": self.m,
            "stripe_size": self.stripe_size,
            "shard_size": self.shard_size,
            "codec": self.codec,
            "storage_prefix": self.storage_prefix,
            "content_sha256": self.content_sha256,
            "manifest_root": self.manifest_root,
            "created_at": self.created_at,
            "writer": self.writer,
            "stripes": [s.to_dict() for s in self.stripes],
        }

    def to_bytes(self) -> bytes:
        """Canonical, self-checksummed serialisation for persistence."""
        payload = _canonical(self.to_dict())
        envelope = {"checksum": _sha256_hex(payload), "payload": payload.decode("ascii")}
        return _canonical(envelope)

    @classmethod
    def from_bytes(cls, raw: bytes) -> "ObjectDescriptor":
        try:
            envelope = json.loads(raw.decode("utf-8"))
            checksum = envelope["checksum"]
            payload = envelope["payload"].encode("ascii")
        except (ValueError, KeyError, TypeError, UnicodeDecodeError, AttributeError) as e:
            raise DescriptorError(f"descriptor envelope is malformed: {e}") from e
        if _sha256_hex(payload) != checksum:
            raise DescriptorError("descriptor checksum mismatch (corrupt or tampered)")
        try:
            data = json.loads(payload.decode("ascii"))
        except ValueError as e:
            raise DescriptorError(f"descriptor payload is malformed: {e}") from e
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, d: dict) -> "ObjectDescriptor":
        try:
            stripes = tuple(StripeManifest.from_dict(s) for s in d["stripes"])
            desc = cls(
                name=str(d["name"]),
                generation=str(d["generation"]),
                length=int(d["length"]),
                k=int(d["k"]),
                m=int(d["m"]),
                stripe_size=int(d["stripe_size"]),
                shard_size=int(d["shard_size"]),
                codec=str(d["codec"]),
                storage_prefix=str(d["storage_prefix"]),
                content_sha256=str(d["content_sha256"]),
                manifest_root=str(d["manifest_root"]),
                created_at=str(d["created_at"]),
                writer=None if d.get("writer") is None else str(d["writer"]),
                stripes=stripes,
                format=str(d["format"]),
            )
        except (KeyError, TypeError, ValueError) as e:
            raise DescriptorError(f"descriptor is missing or has bad fields: {e}") from e
        desc._validate()
        return desc

    # -- internal consistency -----------------------------------------------

    def _validate(self) -> None:
        if self.format != FORMAT_VERSION:
            raise DescriptorError(f"unsupported descriptor format {self.format!r}")
        if not self.name:
            raise DescriptorError("descriptor has empty object name")
        if not 1 <= self.k <= self.m <= 256:
            raise DescriptorError(f"bad erasure parameters k={self.k} m={self.m}")
        if self.stripe_size != self.k * self.shard_size:
            raise DescriptorError("stripe_size != k * shard_size")
        if self.length < 0:
            raise DescriptorError("negative object length")
        expected_stripes = (
            0 if self.length == 0 else (self.length + self.stripe_size - 1) // self.stripe_size
        )
        if len(self.stripes) != expected_stripes:
            raise DescriptorError(
                f"length {self.length} implies {expected_stripes} stripes, "
                f"descriptor carries {len(self.stripes)}"
            )
        total = 0
        for i, s in enumerate(self.stripes):
            if s.index != i:
                raise DescriptorError(f"stripe {i} has wrong index {s.index}")
            if len(s.piece_hashes) != self.m:
                raise DescriptorError(f"stripe {i} does not carry m={self.m} piece hashes")
            if not 1 <= s.length <= self.stripe_size:
                raise DescriptorError(f"stripe {i} has invalid length {s.length}")
            if i < len(self.stripes) - 1 and s.length != self.stripe_size:
                raise DescriptorError(f"non-final stripe {i} is short")
            total += s.length
        if total != self.length:
            raise DescriptorError("stripe lengths do not add up to object length")
        if compute_manifest_root(self.stripes) != self.manifest_root:
            raise DescriptorError("manifest root mismatch (corrupt or tampered)")


def _canonical(obj: object) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
