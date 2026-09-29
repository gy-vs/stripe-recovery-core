"""stripe-recovery-core: integrity-verified erasure-coded stripe kernel.

The library turns a byte stream into an immutable object generation
(descriptor + coded shares on caller-supplied async storage) and reads it
back only after every returned byte has been authenticated against the
descriptor.  Mathematical decoder output is never returned as content.
"""

from __future__ import annotations

from .coding import CodingParams
from .descriptor import ObjectDescriptor, build_descriptor, parse_descriptor
from .errors import (ClosedError, ConditionFailed, DescriptorError,
                     InvalidParams, NotFound, ObjectNotFound, SrcError,
                     StorageError, UnrecoverableError, UnverifiableError)
from .kernel import Kernel, KernelConfig
from .reader import RangeReader
from .repair import RepairCoordinator, RepairHandle
from .results import (KernelResourceSnapshot, ReadReport, ReadStatus,
                      ReaderStats, RepairReport, RepairStats, RepairStatus,
                      ShareStatus, StripeResult, WriterStats)
from .storage import AsyncStorage, FileStorage, MemoryStorage
from .writer import ObjectWriter

__version__ = "0.1.0"

__all__ = [
    "Kernel", "KernelConfig",
    "ObjectWriter", "RangeReader", "RepairHandle", "RepairCoordinator",
    "ObjectDescriptor", "CodingParams",
    "build_descriptor", "parse_descriptor",
    "AsyncStorage", "MemoryStorage", "FileStorage",
    "ReadStatus", "RepairStatus", "ShareStatus",
    "ReadReport", "RepairReport", "StripeResult",
    "ReaderStats", "RepairStats", "WriterStats", "KernelResourceSnapshot",
    "SrcError", "StorageError", "NotFound", "ConditionFailed",
    "DescriptorError", "InvalidParams", "ObjectNotFound", "ClosedError",
    "UnrecoverableError", "UnverifiableError",
    "__version__",
]
