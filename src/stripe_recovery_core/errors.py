"""Typed errors for stripe-recovery-core.

The public interface distinguishes three outcomes that callers care about:

* success                -> result objects (WriteResult / ReadReport / RepairResult)
* unrecoverable content  -> UnrecoverableError (not enough trustworthy shards)
* external storage fault -> StorageError (the caller-provided backend failed)

plus descriptor problems, usage mistakes and cancellation.
"""

from __future__ import annotations

from enum import Enum
from typing import Any


class ErrorKind(str, Enum):
    STORAGE = "storage"
    UNRECOVERABLE = "unrecoverable"
    INTEGRITY = "integrity"
    CANCELLED = "cancelled"
    DESCRIPTOR = "descriptor"
    USAGE = "usage"


class KernelError(Exception):
    """Base class for every error raised by the kernel."""

    kind: ErrorKind = ErrorKind.USAGE

    def __init__(self, message: str, *, detail: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.detail: dict[str, Any] = detail or {}


class StorageError(KernelError):
    """The caller-provided storage backend failed (raise/timeout/...).

    Distinct from "shard absent": an absent shard contributes towards
    UnrecoverableError, a failing backend raises StorageError.
    """

    kind = ErrorKind.STORAGE

    def __init__(
        self,
        message: str,
        *,
        cause: BaseException | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, detail=detail)
        self.__cause__ = cause


class UnrecoverableError(KernelError):
    """Not enough trustworthy shards remained to reconstruct requested content.

    ``detail`` carries the stripe index and the per-shard failure map so the
    caller can see exactly which shards could not participate and why.
    """

    kind = ErrorKind.UNRECOVERABLE


class IntegrityError(KernelError):
    """Decoded bytes failed verification against the descriptor.

    This is defence in depth: shard pieces are hash-checked before decoding,
    so reaching this error means the descriptor itself is inconsistent with
    the codec output. The kernel never delivers unverified bytes.
    """

    kind = ErrorKind.INTEGRITY


class OperationCancelled(KernelError):
    """The operation was cancelled through its own cancel() handle."""

    kind = ErrorKind.CANCELLED


class DescriptorError(KernelError):
    """A descriptor is malformed, tampered with, or incompatible."""

    kind = ErrorKind.DESCRIPTOR


class UsageError(KernelError):
    """The caller violated the API contract (bad ranges, bad lengths, ...)."""

    kind = ErrorKind.USAGE
