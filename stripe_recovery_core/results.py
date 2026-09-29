"""Public result types: statuses, per-share accounting and statistics.

Every read and repair returns enough structured information that callers
never have to infer integrity from "no exception was raised".
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class ReadStatus(str, enum.Enum):
    """Outcome classification for range reads and stripe recovery."""

    CONFIRMED = "confirmed"
    """All requested bytes passed descriptor-bound hash verification."""

    UNRECOVERABLE = "unrecoverable"
    """Fewer than k *authentic* shares were available for a stripe."""

    UNVERIFIABLE = "unverifiable"
    """Shares decoded mathematically but failed integrity verification.

    Reconstructed bytes are deliberately withheld.
    """

    STORAGE_FAILURE = "storage_failure"
    """The caller's storage backend failed; integrity is undetermined."""

    CANCELLED = "cancelled"
    """The operation was cancelled before completion."""

    @property
    def ok(self) -> bool:
        return self is ReadStatus.CONFIRMED


class ShareStatus(str, enum.Enum):
    """Fate of one share within a stripe operation."""

    USED = "used"
    """Fetched, hash-verified and participated in reconstruction."""

    AVAILABLE_NOT_NEEDED = "available_not_needed"
    """Fetched and verified, but not needed to satisfy the request."""

    MISSING = "missing"
    """Storage reported the share absent."""

    CORRUPT = "corrupt"
    """Present, but its leaf digest did not match the manifest."""

    FETCH_FAILED = "fetch_failed"
    """Storage raised an error while fetching this share."""

    REGENERATED = "regenerated"
    """Reconstructed and written back to storage by a repair."""

    REPAIR_ALREADY_PRESENT = "repair_already_present"
    """Repair found the key had appeared meanwhile (CAS lost)."""

    REPAIR_FAILED = "repair_failed"
    """Repair write to storage failed for this share."""

    NOT_ATTEMPTED = "not_attempted"


@dataclass
class StripeResult:
    """Result of attempting one stripe during a range read."""

    stripe_index: int
    status: ReadStatus
    share_statuses: dict[int, ShareStatus] = field(default_factory=dict)
    used_share_indices: tuple[int, ...] = ()
    error: str | None = None

    @property
    def missing(self) -> tuple[int, ...]:
        return tuple(i for i, s in self.share_statuses.items()
                     if s is ShareStatus.MISSING)

    @property
    def corrupt(self) -> tuple[int, ...]:
        return tuple(i for i, s in self.share_statuses.items()
                     if s is ShareStatus.CORRUPT)

    @property
    def fetch_failed(self) -> tuple[int, ...]:
        return tuple(i for i, s in self.share_statuses.items()
                     if s is ShareStatus.FETCH_FAILED)


@dataclass
class ReadReport:
    """Final, checkable summary of a (possibly partial) range read.

    ``confirmed_start`` / ``confirmed_end`` delimit the half-open absolute
    byte range that was both delivered to the consumer and authenticated
    against the descriptor.  Delivered bytes never extend past
    ``confirmed_end``.
    """

    status: ReadStatus
    start: int
    requested_end: int
    confirmed_end: int
    bytes_returned: int
    stripes: tuple[StripeResult, ...] = ()
    used_shares: tuple[tuple[int, int], ...] = ()
    missing_shares: tuple[tuple[int, int], ...] = ()
    corrupt_shares: tuple[tuple[int, int], ...] = ()
    fetch_failed_shares: tuple[tuple[int, int], ...] = ()
    manifest_regenerated: tuple[int, ...] = ()
    error: str | None = None

    @property
    def confirmed_start(self) -> int:
        return self.start

    def raise_for_status(self) -> None:
        """Convenience for callers that prefer exceptions for non-OK reads."""
        from .errors import (StorageError, UnrecoverableError,
                             UnverifiableError)
        if self.status is ReadStatus.CONFIRMED:
            return
        if self.status is ReadStatus.UNRECOVERABLE:
            raise UnrecoverableError(self.error or "not enough authentic shares")
        if self.status is ReadStatus.UNVERIFIABLE:
            raise UnverifiableError(self.error or "integrity verification failed")
        if self.status is ReadStatus.STORAGE_FAILURE:
            raise StorageError(self.error or "storage failure")
        raise RuntimeError(f"read ended with status {self.status.value}: "
                           f"{self.error or ''}")


class RepairStatus(str, enum.Enum):
    REPAIRED = "repaired"
    """All missing shares/manifests were regenerated (or were already there)."""

    UNRECOVERABLE = "unrecoverable"
    """A stripe had fewer than k authentic shares."""

    UNVERIFIABLE = "unverifiable"
    """A stripe decoded but failed verification."""

    STORAGE_FAILURE = "storage_failure"
    CANCELLED = "cancelled"

    @property
    def ok(self) -> bool:
        return self is RepairStatus.REPAIRED


@dataclass
class RepairStripeResult:
    stripe_index: int
    status: RepairStatus
    regenerated_shares: tuple[int, ...] = ()
    already_present_shares: tuple[int, ...] = ()
    missing_before: tuple[int, ...] = ()
    corrupt_shares: tuple[int, ...] = ()
    manifest_regenerated: bool = False
    error: str | None = None


@dataclass
class RepairReport:
    status: RepairStatus
    stripes: tuple[RepairStripeResult, ...] = ()
    shares_regenerated: int = 0
    manifests_regenerated: int = 0
    error: str | None = None

    @property
    def repaired_stripes(self) -> tuple[RepairStripeResult, ...]:
        return tuple(s for s in self.stripes
                     if s.regenerated_shares or s.manifest_regenerated)

    def raise_for_status(self) -> None:
        from .errors import StorageError, UnrecoverableError, UnverifiableError
        if self.status is RepairStatus.REPAIRED:
            return
        if self.status is RepairStatus.UNRECOVERABLE:
            raise UnrecoverableError(self.error or "unrecoverable stripe")
        if self.status is RepairStatus.UNVERIFIABLE:
            raise UnverifiableError(self.error or "verification failed")
        if self.status is RepairStatus.STORAGE_FAILURE:
            raise StorageError(self.error or "storage failure")
        raise RuntimeError(f"repair ended with status {self.status.value}")


@dataclass
class WriterStats:
    bytes_accepted: int = 0
    bytes_uploaded: int = 0
    shares_uploaded: int = 0
    manifests_uploaded: int = 0
    stripes_encoded: int = 0
    inflight_bytes: int = 0
    closed: bool = False
    committed: bool = False
    aborted: bool = False


@dataclass
class ReaderStats:
    bytes_requested: int = 0
    bytes_returned: int = 0
    shares_fetched: int = 0
    share_bytes_fetched: int = 0
    stripes_processed: int = 0
    repairs_spawned: int = 0
    closed: bool = False


@dataclass
class RepairStats:
    stripes_scanned: int = 0
    shares_regenerated: int = 0
    manifests_regenerated: int = 0
    repair_bytes_written: int = 0
    closed: bool = False


@dataclass
class KernelResourceSnapshot:
    """Checkable account of resources owned by the kernel at one instant."""

    active_writers: int = 0
    active_readers: int = 0
    active_repairs: int = 0
    inflight_storage_bytes: int = 0
    inflight_tasks: int = 0
    coders_cached: int = 0
