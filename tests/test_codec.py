"""Codec-level verification: any k-of-m reconstructs, determinism, validation."""

import itertools
import os
import random

import pytest

from stripe_recovery_core.codecs import CauchyRSCodec, CodecError


def test_any_k_of_m_reconstructs():
    codec = CauchyRSCodec()
    rng = random.Random(1234)
    for k, m in [(1, 1), (1, 4), (2, 3), (3, 3), (4, 7), (5, 8), (7, 10)]:
        piece_size = 37
        stripe = os.urandom(k * piece_size)
        pieces = codec.encode(stripe, k, m, piece_size)
        assert len(pieces) == m
        assert all(len(p) == piece_size for p in pieces)
        combos = list(itertools.combinations(range(m), k))
        if len(combos) > 120:
            combos = rng.sample(combos, 120)
        for combo in combos:
            decoded = codec.decode({i: pieces[i] for i in combo}, k, m, piece_size)
            assert decoded == stripe, f"k={k} m={m} combo={combo}"


def test_systematic_first_k_pieces_are_data():
    codec = CauchyRSCodec()
    stripe = os.urandom(4 * 64)
    pieces = codec.encode(stripe, 4, 7, 64)
    assert b"".join(pieces[:4]) == stripe


def test_encode_is_deterministic():
    codec = CauchyRSCodec()
    stripe = os.urandom(4 * 64)
    assert codec.encode(stripe, 4, 7, 64) == codec.encode(stripe, 4, 7, 64)


def test_decode_requires_k_pieces():
    codec = CauchyRSCodec()
    stripe = os.urandom(4 * 32)
    pieces = codec.encode(stripe, 4, 7, 32)
    with pytest.raises(CodecError):
        codec.decode({0: pieces[0], 1: pieces[1], 2: pieces[2]}, 4, 7, 32)


def test_decode_rejects_wrong_piece_size():
    codec = CauchyRSCodec()
    stripe = os.urandom(4 * 32)
    pieces = codec.encode(stripe, 4, 7, 32)
    bad = dict(enumerate(pieces[:4]))
    bad[1] = pieces[1][:-1]
    with pytest.raises(CodecError):
        codec.decode(bad, 4, 7, 32)
