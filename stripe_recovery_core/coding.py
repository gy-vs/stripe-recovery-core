"""Erasure-coding layer: layout rules and a thin, cached zfec wrapper.

The object is divided into stripes of ``block_size * k`` bytes.  Within a
stripe zfec produces ``n = k + m`` equal-sized share blocks; indices
``0 .. k-1`` are systematic (the data blocks themselves), ``k .. n-1``
parity.  Any k authentic shares reconstruct the stripe.

The final stripe is zero-padded to ``k * block_size``; every share block
is therefore exactly ``block_size`` long (zfec requires equal length), and
the object descriptor carries the true length so padding bytes are never
returned to callers.  Padding is verified to be zero after reconstruction,
which closes the "a decode succeeded but said nothing" gap for the tail.
"""

from __future__ import annotations

from dataclasses import dataclass

import zfec

from .errors import InvalidParams

STRIPE_MAGIC = b"SRC1"


@dataclass(frozen=True)
class CodingParams:
    """Immutable erasure-coding parameters of one object."""

    k: int
    m: int
    block_size: int

    @property
    def n(self) -> int:
        return self.k + self.m

    @property
    def stripe_data_size(self) -> int:
        return self.k * self.block_size

    def validate(self) -> None:
        if not isinstance(self.k, int) or not isinstance(self.m, int):
            raise InvalidParams("k and m must be integers")
        if not isinstance(self.block_size, int):
            raise InvalidParams("block_size must be an integer")
        if self.k < 1 or self.m < 0:
            raise InvalidParams("require k >= 1 and m >= 0")
        if self.m == 0 and self.k < 1:
            raise InvalidParams("require k >= 1")
        if self.block_size <= 0:
            raise InvalidParams("block_size must be positive")
        if self.k > 65535 or self.m > 65535:
            raise InvalidParams("k and m must be <= 65535 (zfec GF(2^8))")

    def stripe_count(self, length: int) -> int:
        if length < 0:
            raise InvalidParams("length must be >= 0")
        if length == 0:
            return 1
        return (length + self.stripe_data_size - 1) // self.stripe_data_size

    def stripe_data_length(self, stripe_index: int, total: int) -> int:
        """Original (unpadded) data length of one stripe."""
        if stripe_index < 0:
            raise InvalidParams("stripe_index must be >= 0")
        start = stripe_index * self.stripe_data_size
        if start >= total:
            raise InvalidParams("stripe_index past end of object")
        return min(self.stripe_data_size, total - start)

    def split_stripe(self, stripe_data: bytes) -> list[bytes]:
        """Split one stripe's data into k data blocks with zero tail padding."""
        if len(stripe_data) > self.stripe_data_size:
            raise InvalidParams("stripe data larger than k * block_size")
        padded = stripe_data + b"\x00" * (self.stripe_data_size - len(stripe_data))
        return [padded[i * self.block_size: (i + 1) * self.block_size]
                for i in range(self.k)]


class Coder:
    """Process-wide cache of zfec encoders/decoders per (k, m) layout.

    zfec objects are reusable C extension objects and hold no per-object
    state, so sharing them across objects is safe.
    """

    def __init__(self) -> None:
        self._encoders: dict[tuple[int, int], zfec.Encoder] = {}
        self._decoders: dict[tuple[int, int], zfec.Decoder] = {}

    def encode(self, params: CodingParams, data_blocks: list[bytes]) -> list[bytes]:
        """Return n share blocks (k systematic followed by m parity)."""
        key = (params.k, params.m)
        enc = self._encoders.get(key)
        if enc is None:
            enc = zfec.Encoder(params.k, params.n)
            self._encoders[key] = enc
        return list(enc.encode(data_blocks))

    def decode(self, params: CodingParams, share_blocks: list[bytes],
               share_indices: list[int]) -> list[bytes]:
        """Reconstruct k data blocks from any k authenticated shares.

        Output is ordered by *original* block index regardless of the order
        in which the shares were supplied.
        """
        if len(share_blocks) != params.k or len(share_indices) != params.k:
            raise InvalidParams(
                f"decode requires exactly {params.k} shares, "
                f"got {len(share_blocks)}")
        key = (params.k, params.m)
        dec = self._decoders.get(key)
        if dec is None:
            dec = zfec.Decoder(params.k, params.n)
            self._decoders[key] = dec
        return list(dec.decode(share_blocks, share_indices))
