"""stripe-recovery-core: embeddable erasure-coded striping kernel.

Callers submit a byte stream and receive a persistable ObjectDescriptor plus
a shard set in their own storage; readers stream verified content back, with
explicit reporting of which shards were used and what is confirmed.
"""

from .backend import InMemoryBackend, StorageBackend
from .codecs import CauchyRSCodec, CodecError, StripeCodec
from .config import KernelConfig
from .descriptor import FORMAT_VERSION, ObjectDescriptor, StripeManifest
from .errors import (
    DescriptorError,
    ErrorKind,
    IntegrityError,
    KernelError,
    OperationCancelled,
    StorageError,
    UnrecoverableError,
    UsageError,
)
from .kernel import ReadSession, RepairOperation, StripeKernel
from .models import (
    KernelStats,
    ReadReport,
    RepairResult,
    RepairStatus,
    ResourceReport,
    WriteResult,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # configuration & entry point
    "KernelConfig",
    "StripeKernel",
    # storage
    "StorageBackend",
    "InMemoryBackend",
    # descriptors
    "ObjectDescriptor",
    "StripeManifest",
    "FORMAT_VERSION",
    # operations
    "ReadSession",
    "RepairOperation",
    # results & stats
    "WriteResult",
    "ReadReport",
    "RepairResult",
    "RepairStatus",
    "KernelStats",
    "ResourceReport",
    # codec
    "StripeCodec",
    "CauchyRSCodec",
    "CodecError",
    # errors
    "KernelError",
    "ErrorKind",
    "StorageError",
    "UnrecoverableError",
    "IntegrityError",
    "OperationCancelled",
    "DescriptorError",
    "UsageError",
]
