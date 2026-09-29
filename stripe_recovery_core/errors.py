"""Exception hierarchy for stripe-recovery-core.

The public surface distinguishes three failure classes explicitly:

* :class:`StorageError` -- the caller-supplied storage backend failed
  (transient or permanent).  The object's integrity is not implicated.
* :class:`UnrecoverableError` / the ``unrecoverable`` read status -- not
  enough *authentic* shares were available to reconstruct a stripe.
* :class:`UnverifiableError` / the ``unverifiable`` read status -- shares
  decoded mathematically but failed the cryptographic integrity checks
  bound to the object descriptor.  Bytes are deliberately never returned.
"""

from __future__ import annotations


class SrcError(Exception):
    """Base class for all stripe-recovery-core errors."""


class StorageError(SrcError):
    """An operation against the caller-supplied storage backend failed."""

    def __init__(self, message: str = "", *, key: str | None = None,
                 cause: BaseException | None = None) -> None:
        super().__init__(message or (f"storage failure for key {key!r}"
                                     if key is not None else "storage failure"))
        self.key = key
        self.cause = cause


class NotFound(StorageError):
    """A storage key does not exist.

    Raised by storage backends.  Missing *shares* are not, by themselves,
    an error: erasure coding exists to tolerate them.  A missing descriptor
    or head pointer is surfaced through this class.
    """


class ConditionFailed(StorageError):
    """A put-if-absent condition failed because the key already exists."""


class DescriptorError(SrcError):
    """An object descriptor is malformed, tampered with or incompatible."""


class InvalidParams(SrcError):
    """Invalid coding parameters (e.g. k > n, non-positive block size)."""


class UnrecoverableError(SrcError):
    """A standalone operation (e.g. repair) could not reconstruct a stripe."""


class UnverifiableError(SrcError):
    """Reconstructed bytes failed descriptor-bound integrity verification."""


class ObjectNotFound(SrcError):
    """No committed object is published under the requested name/key."""


class ClosedError(SrcError):
    """An async resource was closed and may no longer be used."""
