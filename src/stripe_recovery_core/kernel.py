"""The striping kernel: put / read / repair over caller-provided storage.

Consistency model
-----------------
* Every ``put`` writes its shards under keys scoped by a fresh *generation*
  id, then publishes the descriptor (generation key first, "latest" pointer
  second — the commit point). Uncommitted generations are unreachable and
  can never be mistaken for a complete object.
* Re-putting the same object name creates a new generation. Old descriptors
  keep addressing their own generation's shards, so a same-name rewrite can
  never change what an existing descriptor reads.
* Repair only writes pieces whose SHA-256 matches the committed descriptor,
  into that descriptor's generation keys. A late repair of an old generation
  therefore cannot touch newer content, and repair can never change what an
  object *means*.

Integrity model
---------------
Raw erasure decode (e.g. zfec) authenticates nothing: mixing shards from two
encodings, or flipping a byte, yields plausible garbage without any error.
This kernel never delivers such output. Every shard piece is checked against
its descriptor hash before decoding, every decoded stripe is checked against
its stripe hash, and a full-object read is additionally checked against the
whole-content hash. Bytes are delivered only after verification.
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from collections.abc import AsyncIterator, Iterable
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from .backend import StorageBackend
from .codecs import CauchyRSCodec, StripeCodec
from .config import KernelConfig
from .descriptor import ObjectDescriptor, StripeManifest, compute_manifest_root
from .errors import (
    DescriptorError,
    IntegrityError,
    KernelError,
    OperationCancelled,
    StorageError,
    UnrecoverableError,
    UsageError,
)
from .models import (
    KernelStats,
    ReadReport,
    RepairResult,
    RepairStatus,
    ResourceReport,
    WriteResult,
)

__all__ = ["StripeKernel", "ReadSession", "RepairOperation"]


# ---------------------------------------------------------------------------
# Storage key layout
# ---------------------------------------------------------------------------

def _quote(name: str) -> str:
    # Keep keys readable while making any object name safe to embed.
    from urllib.parse import quote

    return quote(name, safe="")


def _piece_key(prefix: str, name: str, generation: str, shard: int, stripe: int) -> str:
    return (
        f"{prefix}/objects/{_quote(name)}/gen/{generation}"
        f"/shards/{shard:03d}/{stripe:08d}"
    )


def _descriptor_key(prefix: str, name: str) -> str:
    return f"{prefix}/objects/{_quote(name)}/descriptor"


def _generation_descriptor_key(prefix: str, name: str, generation: str) -> str:
    return f"{prefix}/objects/{_quote(name)}/gen/{generation}/descriptor"


async def _as_async_iter(source: Any) -> AsyncIterator[bytes]:
    if isinstance(source, (bytes, bytearray, memoryview)):
        yield bytes(source)
        return
    if hasattr(source, "__aiter__"):
        async for chunk in source:
            yield _coerce_chunk(chunk)
        return
    if isinstance(source, Iterable):
        for chunk in source:
            yield _coerce_chunk(chunk)
        return
    raise UsageError(
        "source must be bytes or a (sync or async) iterable of bytes, "
        f"got {type(source).__name__}"
    )


def _coerce_chunk(chunk: Any) -> bytes:
    if not isinstance(chunk, (bytes, bytearray, memoryview)):
        raise UsageError(f"source yielded non-bytes chunk: {type(chunk).__name__}")
    return bytes(chunk)


# ---------------------------------------------------------------------------
# Kernel
# ---------------------------------------------------------------------------

class StripeKernel:
    """Embeddable erasure striping kernel.

    Owns no storage of its own: every byte goes through the caller-supplied
    ``backend``. Owns no event-loop policy either — all waiting is plain
    ``await``, so producers and consumers pace each other naturally.
    """

    def __init__(
        self,
        config: KernelConfig | None = None,
        backend: StorageBackend | None = None,
        codec: StripeCodec | None = None,
    ) -> None:
        if backend is None:
            raise UsageError("StripeKernel requires a caller-provided storage backend")
        self.config = config or KernelConfig()
        self.backend = backend
        self.codec: StripeCodec = codec or CauchyRSCodec()
        if self.codec.codec_id != self.config.codec_id:
            # The codec id is written into descriptors; keep them consistent.
            self.config = replace(self.config, codec_id=self.codec.codec_id)
        self._semaphore = asyncio.Semaphore(self.config.max_in_flight_piece_ops)
        self._stats = KernelStats()
        self._active = {"put": 0, "read": 0, "repair": 0}
        self._in_flight_piece_ops = 0
        self._stripe_buffers_held = 0
        self._approx_buffer_bytes = 0
        self._closed = False

    # -- public: stats & resources ------------------------------------------

    @property
    def stats(self) -> KernelStats:
        return replace(self._stats)

    def resource_report(self) -> ResourceReport:
        return ResourceReport(
            active_puts=self._active["put"],
            active_reads=self._active["read"],
            active_repairs=self._active["repair"],
            in_flight_piece_ops=self._in_flight_piece_ops,
            stripe_buffers_held=self._stripe_buffers_held,
            approx_buffer_bytes=self._approx_buffer_bytes,
        )

    async def aclose(self) -> None:
        """Mark the kernel closed. In-flight operations keep their resources
        until they finish; new operations are rejected."""
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise UsageError("kernel is closed")

    # -- public: write --------------------------------------------------------

    async def put(
        self,
        name: str,
        source: Any,
        *,
        length: int | None = None,
        writer: str | None = None,
    ) -> WriteResult:
        """Consume ``source`` (bytes or async/sync iterable of bytes), encode it
        stripe by stripe, write shards, and commit a descriptor.

        The object becomes readable as a complete object only at commit time;
        a failure anywhere before commit leaves no visible object and
        best-effort removes the staged shards.
        """
        self._ensure_open()
        if not isinstance(name, str) or not name:
            raise UsageError("object name must be a non-empty string")
        if length is not None and length < 0:
            raise UsageError("declared length must be >= 0")

        cfg = self.config
        generation = uuid.uuid4().hex
        written_keys: list[str] = []
        stripes: list[StripeManifest] = []
        content_hash = hashlib.sha256()
        total = 0
        bytes_stored = 0
        committed = False
        self._active["put"] += 1
        # Backend failures surface as StorageError (wrapped inside
        # _backend_write); failures of the caller's own source propagate
        # unchanged. Either way, staged shards are cleaned up below.
        try:
            buf = bytearray()
            async for chunk in _as_async_iter(source):
                if not chunk:
                    continue
                buf += chunk
                while len(buf) >= cfg.stripe_size:
                    stripe = bytes(buf[: cfg.stripe_size])
                    del buf[: cfg.stripe_size]
                    bytes_stored += await self._encode_and_write_stripe(
                        name, generation, stripes, stripe, written_keys
                    )
                    content_hash.update(stripe)
                    total += len(stripe)
                    self._stats.bytes_encoded += len(stripe)
                    if length is not None and total > length:
                        raise UsageError(
                            f"source produced more than the declared length {length}"
                        )
            if buf:
                stripe = bytes(buf)
                bytes_stored += await self._encode_and_write_stripe(
                    name, generation, stripes, stripe, written_keys
                )
                content_hash.update(stripe)
                total += len(stripe)
                self._stats.bytes_encoded += len(stripe)
            if length is not None and total != length:
                raise UsageError(
                    f"declared length {length} does not match actual length {total}"
                )

            descriptor = ObjectDescriptor(
                name=name,
                generation=generation,
                length=total,
                k=cfg.k,
                m=cfg.m,
                stripe_size=cfg.stripe_size,
                shard_size=cfg.shard_size,
                codec=cfg.codec_id,
                storage_prefix=cfg.key_prefix,
                content_sha256=content_hash.hexdigest(),
                manifest_root=compute_manifest_root(stripes),
                created_at=datetime.now(timezone.utc).isoformat(),
                writer=writer,
                stripes=tuple(stripes),
            )
            payload = descriptor.to_bytes()
            # Commit point: the generation record first, then the "latest"
            # pointer. Before the pointer lands, nothing can observe the object.
            await self._backend_write(
                _generation_descriptor_key(cfg.key_prefix, name, generation), payload
            )
            await self._backend_write(_descriptor_key(cfg.key_prefix, name), payload)
            committed = True
            self._stats.objects_written += 1
            self._stats.bytes_stored += bytes_stored
            return WriteResult(
                descriptor=descriptor,
                stripe_count=len(stripes),
                shards_written=len(written_keys),
                bytes_stored=bytes_stored,
            )
        finally:
            self._active["put"] -= 1
            if not committed:
                await self._cleanup_keys(written_keys)

    async def _encode_and_write_stripe(
        self,
        name: str,
        generation: str,
        stripes: list[StripeManifest],
        stripe: bytes,
        written_keys: list[str],
    ) -> int:
        cfg = self.config
        index = len(stripes)
        real_len = len(stripe)
        padded = (
            stripe
            if real_len == cfg.stripe_size
            else stripe + b"\x00" * (cfg.stripe_size - real_len)
        )
        self._track_buffers(1, cfg.stripe_size + cfg.m * cfg.shard_size)
        try:
            pieces = self.codec.encode(padded, cfg.k, cfg.m, cfg.shard_size)
            piece_hashes = tuple(hashlib.sha256(p).hexdigest() for p in pieces)

            async def write_one(shard: int) -> None:
                key = _piece_key(cfg.key_prefix, name, generation, shard, index)
                await self._backend_write(key, pieces[shard])
                written_keys.append(key)

            await asyncio.gather(*(write_one(s) for s in range(cfg.m)))
        finally:
            self._track_buffers(-1, -(cfg.stripe_size + cfg.m * cfg.shard_size))
        stripes.append(
            StripeManifest(
                index=index,
                length=real_len,
                sha256=hashlib.sha256(stripe).hexdigest(),
                piece_hashes=piece_hashes,
            )
        )
        return cfg.m * cfg.shard_size

    # -- public: descriptors ----------------------------------------------------

    async def load_descriptor(self, name: str) -> ObjectDescriptor | None:
        """Fetch the latest committed descriptor for ``name`` (None if absent)."""
        self._ensure_open()
        raw = await self._backend_read(_descriptor_key(self.config.key_prefix, name))
        if raw is None:
            return None
        return ObjectDescriptor.from_bytes(raw)

    # -- public: read -----------------------------------------------------------

    def read(
        self,
        descriptor: ObjectDescriptor,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> "ReadSession":
        """Open a streaming read session over a byte range of the object.

        Returns immediately; actual storage access happens as the consumer
        iterates, so a slow consumer never forces the kernel to buffer more
        than one stripe. Use ``async with`` to guarantee resource release.
        """
        self._ensure_open()
        return ReadSession(self, descriptor, offset, length)

    async def read_bytes(
        self,
        descriptor: ObjectDescriptor,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> bytes:
        """Convenience collector built on the streaming path (tests, small
        objects). The streaming session remains the primary interface."""
        out = bytearray()
        async with self.read(descriptor, offset=offset, length=length) as session:
            async for chunk in session:
                out += chunk
        return bytes(out)

    # -- public: repair ---------------------------------------------------------

    def repair(self, descriptor: ObjectDescriptor) -> "RepairOperation":
        """Create (but not start) a repair operation for a descriptor's shards."""
        self._ensure_open()
        return RepairOperation(self, descriptor)

    # -- shared internals -------------------------------------------------------

    def _check_descriptor(self, descriptor: ObjectDescriptor) -> None:
        if not isinstance(descriptor, ObjectDescriptor):
            raise UsageError("expected an ObjectDescriptor")
        if descriptor.codec != self.codec.codec_id:
            raise DescriptorError(
                f"descriptor codec {descriptor.codec!r} does not match kernel "
                f"codec {self.codec.codec_id!r}"
            )
        # Re-validate internal consistency: a descriptor handed to us must be
        # self-consistent before we trust its hashes.
        descriptor._validate()

    async def _piece_op(self, op: Any, *args: Any) -> Any:
        async with self._semaphore:
            self._in_flight_piece_ops += 1
            try:
                return await op(*args)
            finally:
                self._in_flight_piece_ops -= 1

    async def _backend_read(self, key: str) -> bytes | None:
        try:
            return await self._piece_op(self.backend.read, key)
        except KernelError:
            raise
        except Exception as e:
            raise StorageError(f"backend read failed for key {key!r}", cause=e) from e

    async def _backend_write(self, key: str, data: bytes) -> None:
        try:
            await self._piece_op(self.backend.write, key, data)
        except KernelError:
            raise
        except Exception as e:
            raise StorageError(f"backend write failed for key {key!r}", cause=e) from e

    async def _cleanup_keys(self, keys: list[str]) -> None:
        for key in keys:
            try:
                await self.backend.delete(key)
            except Exception:
                pass  # best effort; orphaned staged shards are unreachable anyway

    def _track_buffers(self, count: int, nbytes: int) -> None:
        self._stripe_buffers_held += count
        self._approx_buffer_bytes += nbytes


# ---------------------------------------------------------------------------
# Read session
# ---------------------------------------------------------------------------

class ReadSession:
    """Single-pass async iterator over a verified byte range.

    Bytes are produced only as the consumer asks for them (real backpressure:
    no consumer await, no storage access, no buffering beyond one stripe).
    ``report`` is updated live and remains inspectable after the session ends,
    including after failures.
    """

    def __init__(
        self,
        kernel: StripeKernel,
        descriptor: ObjectDescriptor,
        offset: int,
        length: int | None,
    ) -> None:
        # Attribute defaults first: __del__ must be safe even when the
        # validation below raises.
        self._registered = False
        self._closed = True
        self._cancelled = False
        self._pending: list[bytes] = []
        kernel._check_descriptor(descriptor)
        if offset < 0 or offset > descriptor.length:
            raise UsageError(
                f"offset {offset} out of range for object of length {descriptor.length}"
            )
        if length is None:
            length = descriptor.length - offset
        if length < 0 or offset + length > descriptor.length:
            raise UsageError(
                f"range [{offset}, {offset + length}) exceeds object length "
                f"{descriptor.length}"
            )
        self._kernel = kernel
        self._descriptor = descriptor
        self._offset = offset
        self._length = length
        self.report = ReadReport(
            name=descriptor.name,
            generation=descriptor.generation,
            offset=offset,
            length=length,
        )
        self._stripe_indices = iter(descriptor.stripe_indices_for(offset, length))
        self._content_hash = hashlib.sha256()
        self._closed = False

    # -- lifecycle -----------------------------------------------------------

    async def __aenter__(self) -> "ReadSession":
        self._register()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    def _register(self) -> None:
        if not self._registered and not self._closed:
            self._registered = True
            self._kernel._active["read"] += 1

    def _unregister(self) -> None:
        if self._registered:
            self._registered = False
            self._kernel._active["read"] -= 1

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            self._pending.clear()
            self._unregister()

    def cancel(self) -> None:
        """Cancel this session. Bytes already delivered to the consumer remain
        valid; only future iterations stop. Other sessions are unaffected."""
        self._cancelled = True

    def __del__(self) -> None:  # best-effort accounting if abandoned
        self._unregister()

    # -- iteration -------------------------------------------------------------

    def __aiter__(self) -> "ReadSession":
        self._register()
        return self

    async def __anext__(self) -> bytes:
        if self._closed:
            raise StopAsyncIteration
        self._register()
        if self._cancelled:
            self.report.cancelled = True
            self._kernel._stats.read_sessions_cancelled += 1
            await self.aclose()
            raise OperationCancelled("read session was cancelled")
        while not self._pending:
            try:
                stripe_index = next(self._stripe_indices)
            except StopIteration:
                self._finish()
                raise StopAsyncIteration
            await self._load_stripe(stripe_index)
        return self._pending.pop(0)

    def _finish(self) -> None:
        desc = self._descriptor
        if self._offset == 0 and self._length == desc.length:
            self.report.content_verified = (
                self._content_hash.hexdigest() == desc.content_sha256
            )
            if not self.report.content_verified:
                # Cannot happen without a codec/descriptor bug: every stripe
                # was already hash-verified. Fail closed anyway.
                self._closed = True
                self._unregister()
                raise IntegrityError(
                    "whole-object content hash mismatch after verified stripes"
                )
        self._kernel._stats.read_sessions_completed += 1
        self._kernel._stats.bytes_delivered += self.report.bytes_delivered
        # Auto-close on exhaustion so resources are reclaimed even without
        # an explicit aclose().
        self._closed = True
        self._unregister()

    async def _load_stripe(self, stripe_index: int) -> None:
        desc = self._descriptor
        manifest = desc.stripes[stripe_index]
        k, m, piece_size = desc.k, desc.m, desc.shard_size
        kernel = self._kernel

        good: dict[int, bytes] = {}
        failures: dict[int, str] = {}
        storage_error_msgs: list[str] = []
        next_shard = 0
        kernel._track_buffers(1, desc.stripe_size + k * piece_size)
        try:
            while len(good) < k and next_shard < m:
                if self._cancelled:
                    break
                want = min(k - len(good), m - next_shard)
                batch = list(range(next_shard, next_shard + want))
                next_shard += want
                results = await asyncio.gather(
                    *(self._read_piece(stripe_index, shard) for shard in batch)
                )
                for shard, (piece, error_kind, error_msg) in zip(batch, results):
                    if error_kind is not None:
                        failures[shard] = error_kind
                        if error_kind == "storage_error":
                            storage_error_msgs.append(error_msg or "")
                        continue
                    assert piece is not None
                    if (
                        hashlib.sha256(piece).hexdigest()
                        != manifest.piece_hashes[shard]
                    ):
                        failures[shard] = "hash_mismatch"
                        continue
                    good[shard] = piece

            if self._cancelled:
                self.report.shards_unavailable.update(failures)
                raise OperationCancelled("read session was cancelled")

            if len(good) < k:
                self.report.shards_unavailable.update(failures)
                self.report.storage_errors.extend(storage_error_msgs)
                if len(storage_error_msgs) == m:
                    raise StorageError(
                        f"storage backend failed for every shard of stripe "
                        f"{stripe_index} of object {desc.name!r}",
                        detail={"stripe": stripe_index, "failures": failures},
                    )
                raise UnrecoverableError(
                    f"stripe {stripe_index} of object {desc.name!r}: only "
                    f"{len(good)} of {k} required shards trustworthy",
                    detail={
                        "stripe": stripe_index,
                        "needed": k,
                        "available": len(good),
                        "failures": failures,
                    },
                )

            selected = sorted(good)[:k]
            decoded = kernel.codec.decode(
                {i: good[i] for i in selected}, k, m, piece_size
            )
        finally:
            kernel._track_buffers(-1, -(desc.stripe_size + k * piece_size))

        plaintext = decoded[: manifest.length]
        if hashlib.sha256(plaintext).hexdigest() != manifest.sha256:
            raise IntegrityError(
                f"decoded stripe {stripe_index} of object {desc.name!r} failed "
                f"verification against the descriptor"
            )

        # Deliver only the intersection with the requested range.
        stripe_start = stripe_index * desc.stripe_size
        req_end = self._offset + self._length
        lo = max(self._offset, stripe_start) - stripe_start
        hi = min(req_end, stripe_start + manifest.length) - stripe_start
        data = plaintext[lo:hi]
        self._content_hash.update(data)
        self.report.shards_used.update(selected)
        self.report.shards_unavailable.update(failures)
        self.report.storage_errors.extend(storage_error_msgs)
        self.report.stripes_decoded += 1
        self.report.bytes_delivered += len(data)
        self._confirm_range(stripe_start + lo, stripe_start + hi)
        kernel._stats.stripes_decoded += 1

        chunk_size = kernel.config.read_chunk_size
        self._pending.extend(
            data[i : i + chunk_size] for i in range(0, len(data), chunk_size)
        )

    def _confirm_range(self, lo: int, hi: int) -> None:
        ranges = self.report.confirmed_ranges
        if ranges and ranges[-1][1] == lo:
            ranges[-1] = (ranges[-1][0], hi)
        else:
            ranges.append((lo, hi))

    async def _read_piece(
        self, stripe_index: int, shard: int
    ) -> tuple[bytes | None, str | None, str | None]:
        desc = self._descriptor
        key = _piece_key(
            desc.storage_prefix, desc.name, desc.generation, shard, stripe_index
        )
        try:
            piece = await self._kernel._piece_op(self._kernel.backend.read, key)
        except Exception as e:  # backend failure: recorded, not fatal per-piece
            return None, "storage_error", f"shard {shard} stripe {stripe_index}: {e!r}"
        if piece is None:
            return None, "missing", None
        return piece, None, None


# ---------------------------------------------------------------------------
# Repair operation
# ---------------------------------------------------------------------------

class RepairOperation:
    """Restores missing/corrupted shard pieces of one committed generation.

    Repair is strictly content-preserving: it only writes pieces whose hash
    matches the descriptor, only into that generation's keys. It never touches
    the descriptor, never deletes, and can run concurrently with reads and
    with newer writes of the same object name.
    """

    def __init__(self, kernel: StripeKernel, descriptor: ObjectDescriptor) -> None:
        kernel._check_descriptor(descriptor)
        self._kernel = kernel
        self._descriptor = descriptor
        self._cancel_event = asyncio.Event()
        self._started = False
        self.result: RepairResult | None = None

    def cancel(self) -> None:
        """Request cancellation; takes effect at the next stripe boundary."""
        self._cancel_event.set()

    async def run(self) -> RepairResult:
        if self._started:
            raise UsageError("a RepairOperation can only be run once")
        self._started = True
        kernel = self._kernel
        desc = self._descriptor
        kernel._active["repair"] += 1
        kernel._stats.repair_runs += 1
        sem = asyncio.Semaphore(kernel.config.repair_stripe_concurrency)
        try:
            async def process(index: int) -> "_StripeRepairOutcome | None":
                if self._cancel_event.is_set():
                    return None
                async with sem:
                    if self._cancel_event.is_set():
                        return None
                    return await self._repair_stripe(index)

            raw = await asyncio.gather(
                *(process(i) for i in range(desc.stripe_count)),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            self.result = self._build_result([], cancelled=True)
            raise
        finally:
            kernel._active["repair"] -= 1

        outcomes: list[_StripeRepairOutcome] = []
        for item in raw:
            if item is None:
                continue
            if isinstance(item, BaseException):
                # Unexpected kernel/codec bug: surface it rather than hide it.
                raise item
            outcomes.append(item)
        cancelled = self._cancel_event.is_set() or len(outcomes) < desc.stripe_count
        self.result = self._build_result(outcomes, cancelled=cancelled)
        return self.result

    def _build_result(
        self, outcomes: list["_StripeRepairOutcome"], *, cancelled: bool
    ) -> RepairResult:
        desc = self._descriptor
        repaired: list[tuple[int, int]] = []
        unrecoverable: list[int] = []
        write_failures: list[tuple[int, int, str]] = []
        storage_errors: list[str] = []
        verified = 0
        for o in outcomes:
            verified += o.verified
            repaired.extend((o.index, s) for s in o.repaired_shards)
            if o.unrecoverable:
                unrecoverable.append(o.index)
            write_failures.extend((o.index, s, msg) for s, msg in o.write_failures)
            storage_errors.extend(o.storage_errors)
        repaired.sort()
        unrecoverable.sort()
        if cancelled:
            status = RepairStatus.CANCELLED
        elif unrecoverable or write_failures:
            status = RepairStatus.PARTIAL
        else:
            status = RepairStatus.COMPLETED
        self._kernel._stats.pieces_repaired += len(repaired)
        return RepairResult(
            name=desc.name,
            generation=desc.generation,
            status=status,
            stripes_total=desc.stripe_count,
            stripes_processed=len(outcomes),
            pieces_verified=verified,
            pieces_repaired=repaired,
            unrecoverable_stripes=unrecoverable,
            write_failures=write_failures,
            storage_errors=storage_errors,
        )

    async def _repair_stripe(self, index: int) -> "_StripeRepairOutcome":
        kernel = self._kernel
        desc = self._descriptor
        manifest = desc.stripes[index]
        k, m, piece_size = desc.k, desc.m, desc.shard_size
        outcome = _StripeRepairOutcome(index=index)

        async def read_one(shard: int) -> tuple[int, bytes | None]:
            key = _piece_key(
                desc.storage_prefix, desc.name, desc.generation, shard, index
            )
            try:
                return shard, await kernel._piece_op(kernel.backend.read, key)
            except Exception as e:
                outcome.storage_errors.append(f"shard {shard} stripe {index}: {e!r}")
                return shard, None

        responses = await asyncio.gather(*(read_one(s) for s in range(m)))
        good: dict[int, bytes] = {}
        missing: list[int] = []
        for shard, piece in responses:
            if piece is None:
                missing.append(shard)
            elif hashlib.sha256(piece).hexdigest() != manifest.piece_hashes[shard]:
                missing.append(shard)  # corrupted: will be rewritten correctly
            else:
                good[shard] = piece
        outcome.verified = len(good)
        if not missing:
            return outcome
        if len(good) < k:
            outcome.unrecoverable = True
            return outcome

        kernel._track_buffers(1, desc.stripe_size + k * piece_size)
        try:
            decoded = kernel.codec.decode(
                {i: good[i] for i in sorted(good)[:k]}, k, m, piece_size
            )
        finally:
            kernel._track_buffers(-1, -(desc.stripe_size + k * piece_size))
        plaintext = decoded[: manifest.length]
        if hashlib.sha256(plaintext).hexdigest() != manifest.sha256:
            # Verified pieces decoded to a stripe that fails verification:
            # descriptor/codec inconsistency. Refuse to "repair" towards it.
            outcome.unrecoverable = True
            outcome.storage_errors.append(
                f"stripe {index}: decoded plaintext fails descriptor hash"
            )
            return outcome
        padded = plaintext + b"\x00" * (desc.stripe_size - manifest.length)
        pieces = kernel.codec.encode(padded, k, m, piece_size)

        for shard in missing:
            piece = pieces[shard]
            if hashlib.sha256(piece).hexdigest() != manifest.piece_hashes[shard]:
                # Defence in depth: never write bytes the descriptor disowns.
                outcome.unrecoverable = True
                continue
            key = _piece_key(
                desc.storage_prefix, desc.name, desc.generation, shard, index
            )
            try:
                await kernel._piece_op(kernel.backend.write, key, piece)
                outcome.repaired_shards.append(shard)
            except Exception as e:
                outcome.write_failures.append((shard, repr(e)))
        return outcome


class _StripeRepairOutcome:
    __slots__ = (
        "index",
        "verified",
        "repaired_shards",
        "unrecoverable",
        "write_failures",
        "storage_errors",
    )

    def __init__(self, index: int) -> None:
        self.index = index
        self.verified = 0
        self.repaired_shards: list[int] = []
        self.unrecoverable = False
        self.write_failures: list[tuple[int, str]] = []
        self.storage_errors: list[str] = []
