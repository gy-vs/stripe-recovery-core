"""Missing shards, corrupted shards, mixed generations, storage outages.

These tests pin down the behaviour raw zfec could not provide: damaged or
foreign shard bytes are detected and excluded, recovery proceeds from
trustworthy shards, and when there are not enough of those the caller gets a
typed error — never unauthenticated bytes.
"""

import pytest

from stripe_recovery_core import (
    ErrorKind,
    StorageError,
    UnrecoverableError,
)
from util import FaultBackend, make_kernel, pattern_bytes, piece_key, run


def _flip_byte(backend, key):
    data = bytearray(backend._data[key])
    data[5] ^= 0xFF
    backend._data[key] = bytes(data)


def test_missing_shards_within_parity_tolerance():
    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(1003)
        desc = (await kernel.put("obj", data)).descriptor
        # remove shards 1 and 2 of every stripe (m-k = 3 tolerated)
        for stripe in range(desc.stripe_count):
            for shard in (1, 2):
                await backend.delete(piece_key(kernel, "obj", desc.generation, shard, stripe))

        async with kernel.read(desc) as session:
            got = b"".join([c async for c in session])
        assert got == data
        report = session.report
        assert report.shards_used == {0, 3, 4, 5}
        assert report.shards_unavailable == {1: "missing", 2: "missing"}
        assert report.content_verified is True
    run(go())


def test_too_many_missing_shards_is_unrecoverable_not_garbage():
    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(1003)
        desc = (await kernel.put("obj", data)).descriptor
        # stripe 1 loses 4 shards: below k=4, unrecoverable
        for shard in (3, 4, 5, 6):
            await backend.delete(piece_key(kernel, "obj", desc.generation, shard, 1))

        session = kernel.read(desc)
        with pytest.raises(UnrecoverableError) as exc:
            async with session:
                async for _ in session:
                    pass
        assert exc.value.kind == ErrorKind.UNRECOVERABLE
        assert exc.value.detail["stripe"] == 1
        assert exc.value.detail["failures"] == {3: "missing", 4: "missing", 5: "missing", 6: "missing"}
        # stripe 0 was delivered and confirmed before the failure; nothing
        # beyond it was emitted, and certainly no wrong bytes
        report = session.report
        assert report.confirmed_ranges == [(0, 256)]
        assert report.bytes_delivered == 256
        assert not report.fully_confirmed
    run(go())


def test_single_flipped_byte_is_detected_and_recovered():
    """The zfec pain point: one flipped byte must not silently poison output."""

    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(1003)
        desc = (await kernel.put("obj", data)).descriptor
        _flip_byte(backend, piece_key(kernel, "obj", desc.generation, 0, 0))

        async with kernel.read(desc) as session:
            got = b"".join([c async for c in session])
        assert got == data  # correct bytes, recovered from other shards
        report = session.report
        assert report.shards_unavailable == {0: "hash_mismatch"}
        # stripe 0 was decoded from shards 1..4 (shard 0 excluded); the
        # healthy stripes still used shard 0, hence the union below
        assert report.shards_used == {0, 1, 2, 3, 4}
        assert report.content_verified is True
    run(go())


def test_mixed_generation_shards_are_detected_not_decoded():
    """Mixing shards from two writes of the same name: raw zfec returns bytes
    belonging to neither input. The kernel must detect, exclude, recover."""

    async def go():
        kernel, backend = make_kernel()
        data_v1 = pattern_bytes(1003, seed=1)
        data_v2 = pattern_bytes(1003, seed=2)
        d1 = (await kernel.put("obj", data_v1)).descriptor
        d2 = (await kernel.put("obj", data_v2)).descriptor

        # simulate an operator accident: one v1 piece lands in a v2 slot
        foreign = backend._data[piece_key(kernel, "obj", d1.generation, 1, 0)]
        backend._data[piece_key(kernel, "obj", d2.generation, 1, 0)] = foreign

        async with kernel.read(d2) as session:
            got = b"".join([c async for c in session])
        assert got == data_v2  # still exactly v2, never a blend
        assert session.report.shards_unavailable == {1: "hash_mismatch"}

        # and v1 remains perfectly readable through its own descriptor
        assert await kernel.read_bytes(d1) == data_v1
    run(go())


def test_mixed_generation_beyond_parity_fails_closed():
    async def go():
        kernel, backend = make_kernel()
        d1 = (await kernel.put("obj", pattern_bytes(1003, seed=1))).descriptor
        d2 = (await kernel.put("obj", pattern_bytes(1003, seed=2))).descriptor

        # 4 of 7 pieces of v2's stripe 0 replaced by v1's: unrecoverable
        for shard in (0, 1, 2, 3):
            backend._data[piece_key(kernel, "obj", d2.generation, shard, 0)] = \
                backend._data[piece_key(kernel, "obj", d1.generation, shard, 0)]

        session = kernel.read(d2)
        with pytest.raises(UnrecoverableError):
            async with session:
                async for _ in session:
                    pass
        # stripe 0 failed immediately: zero bytes delivered, zero chance of
        # the caller mistaking foreign content for their object
        assert session.report.bytes_delivered == 0
    run(go())


def test_storage_outage_is_distinct_from_unrecoverable():
    async def go():
        backend = FaultBackend()
        kernel, _ = make_kernel(backend)
        data = pattern_bytes(600)
        desc = (await kernel.put("obj", data)).descriptor

        # every shard read raises -> StorageError, kind=storage
        backend.fail_reads = True
        with pytest.raises(StorageError) as exc:
            await kernel.read_bytes(desc)
        assert exc.value.kind == ErrorKind.STORAGE

        # every shard absent -> UnrecoverableError, kind=unrecoverable
        backend.fail_reads = False
        for key in list(backend.keys()):
            if "/shards/" in key:
                await backend.delete(key)
        with pytest.raises(UnrecoverableError) as exc2:
            await kernel.read_bytes(desc)
        assert exc2.value.kind == ErrorKind.UNRECOVERABLE
    run(go())


def test_partial_storage_failure_still_recovers_and_reports():
    async def go():
        backend = FaultBackend()
        kernel, _ = make_kernel(backend)
        data = pattern_bytes(600)
        desc = (await kernel.put("obj", data)).descriptor

        # shard 0's storage node errors out; others are healthy
        real_read = backend.read

        async def flaky(key):
            if "/shards/000/" in key:
                raise RuntimeError("node down")
            return await real_read(key)

        backend.read = flaky  # type: ignore[assignment]
        async with kernel.read(desc) as session:
            got = b"".join([c async for c in session])
        assert got == data
        report = session.report
        assert report.shards_unavailable == {0: "storage_error"}
        assert report.storage_errors  # details preserved for the caller
        assert report.content_verified is True
    run(go())


def test_tampered_descriptor_fails_closed_on_read():
    """A descriptor whose integrity basis was altered cannot be used to
    vouch for content."""

    import dataclasses

    from stripe_recovery_core import DescriptorError, IntegrityError

    async def go():
        kernel, _ = make_kernel()
        data = pattern_bytes(600)
        desc = (await kernel.put("obj", data)).descriptor

        # tampering with the content hash breaks whole-object verification
        evil = dataclasses.replace(desc, content_sha256="ff" * 32)
        with pytest.raises(IntegrityError):
            await kernel.read_bytes(evil)

        # tampering with a stripe hash breaks descriptor self-consistency
        bad_stripes = list(desc.stripes)
        bad_stripes[0] = dataclasses.replace(bad_stripes[0], sha256="00" * 32)
        evil2 = dataclasses.replace(desc, stripes=tuple(bad_stripes))
        with pytest.raises(DescriptorError):
            kernel.read(evil2)
    run(go())
