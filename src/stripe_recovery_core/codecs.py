"""Erasure codec: systematic Cauchy Reed-Solomon over GF(2^8).

The kernel treats the codec as a pluggable component (see ``StripeCodec``);
this module ships a dependency-free pure-Python implementation so the kernel
works standalone. The construction mirrors the well-known zfec approach:

* encoding matrix ``G`` is ``m x k``: the top ``k`` rows are the identity
  (systematic: the first k pieces are the stripe itself, split into shards),
  the bottom ``m - k`` rows form a Cauchy matrix ``C[j][i] = 1 / (x_j + y_i)``
  over GF(2^8) with distinct ``x_j``/``y_i``;
* every square submatrix of a Cauchy matrix is invertible, therefore *any*
  ``k`` of the ``m`` pieces reconstruct the stripe.

The codec is deterministic: encoding the same stripe twice yields
byte-identical pieces. Integrity is *not* the codec's job — the kernel
verifies every piece and every decoded stripe against descriptor hashes, so
mixed or corrupted pieces are detected rather than silently decoded into
plausible-looking garbage (which is what raw zfec ``decode`` would return).
"""

from __future__ import annotations

from typing import Mapping, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# GF(2^8) arithmetic, polynomial basis with modulus x^8+x^4+x^3+x^2+1 (0x11d).
# ---------------------------------------------------------------------------

_EXP = [0] * 512
_LOG = [0] * 256
_x = 1
for _i in range(255):
    _EXP[_i] = _x
    _LOG[_x] = _i
    _x <<= 1
    if _x & 0x100:
        _x ^= 0x11D
for _i in range(255, 512):
    _EXP[_i] = _EXP[_i - 255]


def _gf_mul(a: int, b: int) -> int:
    if a == 0 or b == 0:
        return 0
    return _EXP[_LOG[a] + _LOG[b]]


def _gf_inv(a: int) -> int:
    # a != 0
    return _EXP[255 - _LOG[a]]


# Per-coefficient multiplication tables so a whole block can be multiplied by
# one GF coefficient with a single bytes.translate call (C speed).
_MUL_TABLES: tuple[bytes, ...] = tuple(
    bytes(_gf_mul(c, i) for i in range(256)) for c in range(256)
)


def _xor_into(dst: bytearray, src: bytes) -> None:
    n = len(src)
    dst[:] = (int.from_bytes(dst, "little") ^ int.from_bytes(src, "little")).to_bytes(
        n, "little"
    )


def _invert_matrix(matrix: list[list[int]]) -> list[list[int]]:
    """Gauss-Jordan inversion over GF(2^8). Matrix must be non-singular."""
    n = len(matrix)
    aug = [
        row[:] + [1 if i == j else 0 for j in range(n)]
        for i, row in enumerate(matrix)
    ]
    for col in range(n):
        pivot = -1
        for r in range(col, n):
            if aug[r][col] != 0:
                pivot = r
                break
        if pivot < 0:
            raise CodecError("singular decoding matrix (codec bug or bad indices)")
        aug[col], aug[pivot] = aug[pivot], aug[col]
        inv = _gf_inv(aug[col][col])
        aug[col] = [_gf_mul(v, inv) for v in aug[col]]
        for r in range(n):
            if r != col and aug[r][col] != 0:
                factor = aug[r][col]
                aug[r] = [v ^ _gf_mul(factor, w) for v, w in zip(aug[r], aug[col])]
    return [row[n:] for row in aug]


def _cauchy_row(piece_index: int, k: int) -> list[int]:
    """Row of the encoding matrix for ``piece_index``.

    Indices 0..k-1 are systematic (unit rows); indices k..m-1 are Cauchy
    parity rows with x = piece_index, y = i, i.e. 1 / (piece_index + i).
    x and y ranges are disjoint because piece_index >= k > i for all i.
    """
    if piece_index < k:
        row = [0] * k
        row[piece_index] = 1
        return row
    x = piece_index
    return [_gf_inv(x ^ i) for i in range(k)]


class CodecError(Exception):
    """Raised for codec-internal failures (never for untrusted input)."""


@runtime_checkable
class StripeCodec(Protocol):
    """Interface the kernel relies on. Implementations must be deterministic."""

    codec_id: str

    def encode(self, stripe: bytes, k: int, m: int, piece_size: int) -> list[bytes]:
        """Split+encode ``stripe`` (exactly ``k * piece_size`` bytes, zero-padded
        by the caller) into ``m`` pieces of ``piece_size`` bytes each."""
        ...

    def decode(
        self, pieces: Mapping[int, bytes], k: int, m: int, piece_size: int
    ) -> bytes:
        """Reconstruct the ``k * piece_size`` byte stripe from any ``k`` pieces
        given as ``{piece_index: piece_bytes}``."""
        ...


class CauchyRSCodec:
    """Pure-Python systematic Cauchy Reed-Solomon codec."""

    codec_id = "rs-cauchy-gf256-v1"

    def encode(self, stripe: bytes, k: int, m: int, piece_size: int) -> list[bytes]:
        if len(stripe) != k * piece_size:
            raise CodecError(
                f"stripe must be exactly k*piece_size={k * piece_size} bytes, "
                f"got {len(stripe)}"
            )
        if not 1 <= k <= m <= 256:
            raise CodecError(f"unsupported parameters k={k} m={m}")
        data = [stripe[i * piece_size : (i + 1) * piece_size] for i in range(k)]
        pieces: list[bytes] = list(data)
        for p in range(k, m):
            row = _cauchy_row(p, k)
            acc = bytearray(piece_size)
            for i in range(k):
                coef = row[i]
                if coef:
                    _xor_into(acc, data[i].translate(_MUL_TABLES[coef]))
            pieces.append(bytes(acc))
        return pieces

    def decode(
        self, pieces: Mapping[int, bytes], k: int, m: int, piece_size: int
    ) -> bytes:
        if len(pieces) < k:
            raise CodecError(f"need at least {k} pieces, got {len(pieces)}")
        indices = sorted(pieces)[:k]
        for idx in indices:
            if not 0 <= idx < m:
                raise CodecError(f"piece index {idx} out of range for m={m}")
            if len(pieces[idx]) != piece_size:
                raise CodecError(
                    f"piece {idx} has {len(pieces[idx])} bytes, expected {piece_size}"
                )
        matrix = [_cauchy_row(idx, k) for idx in indices]
        inverse = _invert_matrix(matrix)
        out = bytearray(k * piece_size)
        for i in range(k):
            acc = bytearray(piece_size)
            for r in range(k):
                coef = inverse[i][r]
                if coef:
                    _xor_into(acc, pieces[indices[r]].translate(_MUL_TABLES[coef]))
            out[i * piece_size : (i + 1) * piece_size] = acc
        return bytes(out)


def default_codec() -> CauchyRSCodec:
    return CauchyRSCodec()
