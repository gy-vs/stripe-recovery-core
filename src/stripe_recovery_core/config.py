"""Kernel configuration.

Memory usage of the kernel scales with these knobs, never with object size:

* write path holds roughly ``stripe_size + m * shard_size`` bytes per stripe
  being encoded (one stripe of plaintext plus its m encoded pieces);
* read path holds roughly ``stripe_size + k * shard_size`` bytes per stripe
  being decoded;
* ``stripe_size`` is derived as ``k * shard_size`` so both "shard size" and
  "stripe size" are effectively configurable.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import UsageError

DEFAULT_CODEC_ID = "rs-cauchy-gf256-v1"


@dataclass(frozen=True)
class KernelConfig:
    # Erasure parameters: any k of m shards reconstruct a stripe.
    k: int = 4
    m: int = 7
    # Bytes of each shard piece per stripe. stripe_size == k * shard_size.
    shard_size: int = 256 * 1024
    # Maximum chunk size delivered to read consumers.
    read_chunk_size: int = 256 * 1024
    # Bound on concurrent piece reads/writes against the backend.
    max_in_flight_piece_ops: int = 16
    # Bound on stripes repaired concurrently by one repair operation.
    repair_stripe_concurrency: int = 4
    # Root prefix for all storage keys produced by this kernel.
    key_prefix: str = "src"
    # Codec identifier written into descriptors.
    codec_id: str = DEFAULT_CODEC_ID

    def __post_init__(self) -> None:
        if not 1 <= self.k <= self.m <= 256:
            raise UsageError(
                f"require 1 <= k <= m <= 256, got k={self.k} m={self.m}"
            )
        if self.shard_size < 1:
            raise UsageError("shard_size must be >= 1")
        if self.read_chunk_size < 1:
            raise UsageError("read_chunk_size must be >= 1")
        if self.max_in_flight_piece_ops < 1:
            raise UsageError("max_in_flight_piece_ops must be >= 1")
        if self.repair_stripe_concurrency < 1:
            raise UsageError("repair_stripe_concurrency must be >= 1")
        if not self.key_prefix or "/" in self.key_prefix:
            raise UsageError("key_prefix must be a non-empty single path segment")

    @property
    def stripe_size(self) -> int:
        return self.k * self.shard_size
