"""Public result and statistics models."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .descriptor import ObjectDescriptor


class RepairStatus(str, Enum):
    COMPLETED = "completed"  # every piece present and verified (after repairs)
    PARTIAL = "partial"      # some stripes unrecoverable and/or some writes failed
    CANCELLED = "cancelled"  # cancelled before finishing; partial progress reported


@dataclass
class WriteResult:
    """Returned once a put has committed."""

    descriptor: "ObjectDescriptor"
    stripe_count: int
    shards_written: int
    bytes_stored: int

    @property
    def content_sha256(self) -> str:
        return self.descriptor.content_sha256


@dataclass
class ReadReport:
    """Live, inspectable account of a read session.

    ``confirmed_ranges`` lists the byte ranges whose content was verified
    against descriptor hashes and delivered. ``shards_used`` /
    ``shards_unavailable`` say exactly which shards participated and which
    could not (with reasons: "missing", "hash_mismatch", "storage_error").
    """

    name: str
    generation: str
    offset: int
    length: int
    bytes_delivered: int = 0
    confirmed_ranges: list[tuple[int, int]] = field(default_factory=list)
    shards_used: set[int] = field(default_factory=set)
    shards_unavailable: dict[int, str] = field(default_factory=dict)
    stripes_decoded: int = 0
    storage_errors: list[str] = field(default_factory=list)
    # True/False once the whole object was delivered and the content hash was
    # checked; None for partial-range reads.
    content_verified: bool | None = None
    cancelled: bool = False

    @property
    def fully_confirmed(self) -> bool:
        """Every requested byte was delivered from hash-verified stripes."""
        return self.bytes_delivered == self.length


@dataclass
class RepairResult:
    """Outcome of a repair run. Never implies the object content changed:

    repair only ever writes bytes that hash-match the committed descriptor,
    into keys scoped to that descriptor's generation."""

    name: str
    generation: str
    status: RepairStatus
    stripes_total: int
    stripes_processed: int
    pieces_verified: int
    pieces_repaired: list[tuple[int, int]] = field(default_factory=list)  # (stripe, shard)
    unrecoverable_stripes: list[int] = field(default_factory=list)
    write_failures: list[tuple[int, int, str]] = field(default_factory=list)  # (stripe, shard, error)
    storage_errors: list[str] = field(default_factory=list)


@dataclass
class KernelStats:
    """Cumulative, verifiable counters since kernel creation."""

    objects_written: int = 0
    bytes_encoded: int = 0
    bytes_stored: int = 0
    read_sessions_completed: int = 0
    read_sessions_cancelled: int = 0
    bytes_delivered: int = 0
    stripes_decoded: int = 0
    repair_runs: int = 0
    pieces_repaired: int = 0


@dataclass(frozen=True)
class ResourceReport:
    """Point-in-time resource ownership of the kernel.

    All counts return to zero once operations finish or are cancelled and
    closed; a non-zero value pinpoints exactly which kind of operation still
    holds resources."""

    active_puts: int
    active_reads: int
    active_repairs: int
    in_flight_piece_ops: int
    stripe_buffers_held: int
    approx_buffer_bytes: int
