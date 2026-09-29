"""Authenticated stripe recovery primitives shared by reads and repair.

Golden rule: **zfec output is never trusted**.  A fetched share is only a
candidate until its content digest matches the descriptor-authenticated
manifest at its position.  A decoder result is only data once every
reconstructed block's content digest re-matches the manifest (or, when
the manifest itself is missing, once a freshly encoded manifest hashes to
the stripe root recorded in the descriptor), and tail padding is zero.
"""

from __future__ import annotations

import asyncio

from .coding import CodingParams
from .crypto import (content_digest, manifest_blob, parse_manifest,
                     stripe_root)
from .descriptor import ObjectDescriptor
from .errors import NotFound, StorageError
from .results import (ReadStatus, RepairStatus, RepairStripeResult,
                      ShareStatus, StripeResult)
from .storage import keys


# Dynamic attributes attached to StripeResult for internal hand-off.
_EXTRA_ATTRS = ("data_blocks", "manifest_leaves", "all_blocks",
                "manifest_regenerated")


def _blank_result(stripe_index: int, n: int) -> StripeResult:
    res = StripeResult(
        stripe_index=stripe_index, status=ReadStatus.CONFIRMED,
        share_statuses={j: ShareStatus.NOT_ATTEMPTED for j in range(n)})
    for attr in _EXTRA_ATTRS:
        setattr(res, attr, None)
    res.manifest_regenerated = False
    return res


async def _safe_get(storage, key: str):
    """Return (state, payload-or-message)."""
    try:
        return "ok", await storage.get(key)
    except NotFound:
        return "missing", None
    except StorageError as exc:
        return "failed", f"{type(exc).__name__}: {exc}"
    except Exception as exc:  # noqa: BLE001 - normalize backend errors
        return "failed", f"{type(exc).__name__}: {exc}"


async def _get_manifest(storage, desc: ObjectDescriptor, stripe_index: int):
    state, payload = await _safe_get(
        storage, keys.manifest_key(desc, stripe_index))
    if state == "ok":
        try:
            return "ok", parse_manifest(payload, desc.n)
        except ValueError:
            return "malformed", None
    return state, payload


def _fail(result: StripeResult, status: ReadStatus, error: str) -> StripeResult:
    result.status = status
    result.error = error
    result.data_blocks = {}
    result.manifest_leaves = None
    result.all_blocks = None
    return result


def _check_tail_padding(desc: ObjectDescriptor, stripe_index: int,
                        blocks_by_index: dict[int, bytes]) -> bool:
    """The final stripe's declared padding bytes must all be zero."""
    params: CodingParams = desc.params
    if stripe_index != desc.stripe_count - 1:
        return True
    real_len = desc.length - stripe_index * params.stripe_data_size
    if real_len < 0:
        return False
    start_block = real_len // params.block_size
    off = real_len % params.block_size
    if off != 0:
        block = blocks_by_index.get(start_block)
        if block is not None and block[off:] != b"\x00" * (
                params.block_size - off):
            return False
    for b_idx in range(start_block + 1, params.k):
        block = blocks_by_index.get(b_idx)
        if block is not None and block != b"\x00" * params.block_size:
            return False
    return True


async def _fetch_authenticated(storage, desc: ObjectDescriptor,
                               stripe_index: int, indices: list[int],
                               leaves: list[bytes],
                               statuses: dict[int, ShareStatus],
                               counter: dict[str, int],
                               *, present_state: ShareStatus = ShareStatus.USED
                               ) -> dict[int, bytes]:
    """Fetch indices and keep only content-digest-authentic shares.

    Two distinct faults must be distinguished:

    * the blob key named by the manifest is absent -> the position is
      MISSING;
    * storage returns bytes whose digest does not match the manifest at
      that position -> CORRUPT.  Because storage is content-addressed,
      valid blocks are self-identifying: we attribute a misplaced block to
      whichever manifest position expects its digest, and a position whose
      expected bytes came back wrong is marked CORRUPT rather than masking
      that as MISSING.
    """

    async def one(j: int):
        state, payload = await _safe_get(
            storage, keys.blob_key_for_digest(leaves[j]))
        if state != "ok":
            if state == "missing":
                statuses[j] = ShareStatus.MISSING
            else:
                statuses[j] = ShareStatus.FETCH_FAILED
                counter["error"] = payload
            return None
        counter["fetches"] += 1
        counter["bytes"] += len(payload)
        if len(payload) != desc.block_size:
            statuses[j] = ShareStatus.CORRUPT
            return None
        if content_digest(payload) == leaves[j]:
            statuses[j] = present_state
            return j, payload
        # Key/content invariant broken (e.g. bit rot on non-validating
        # media).  If these bytes are in fact another authentic block of
        # this stripe, attribute them there; otherwise the position is
        # corrupt and the bytes are unusable at their own position.
        actual = content_digest(payload)
        for other, expected in enumerate(leaves):
            if actual == expected and statuses.get(other) in (
                    ShareStatus.NOT_ATTEMPTED, ShareStatus.MISSING):
                statuses[j] = ShareStatus.CORRUPT
                statuses[other] = present_state
                return other, payload
        statuses[j] = ShareStatus.CORRUPT
        return None

    pairs = await asyncio.gather(*(one(j) for j in indices))
    return {j: data for pair in pairs if pair is not None
            for j, data in [pair]}


async def plan_stripe_read(storage, coder, desc: ObjectDescriptor,
                           stripe_index: int,
                           needed_data_blocks: set[int] | None,
                           counter: dict[str, int]) -> StripeResult:
    """Recover one stripe with full per-share accounting; never mutates."""
    params: CodingParams = desc.params
    statuses = {j: ShareStatus.NOT_ATTEMPTED for j in range(desc.n)}
    result = _blank_result(stripe_index, desc.n)
    result.share_statuses = statuses
    needed = (set(range(params.k)) if needed_data_blocks is None
              else set(needed_data_blocks))

    mstate, mval = await _get_manifest(storage, desc, stripe_index)
    if mstate == "malformed":
        return _fail(result, ReadStatus.UNVERIFIABLE,
                     f"stripe {stripe_index}: malformed manifest")
    if mstate == "failed":
        return _fail(result, ReadStatus.STORAGE_FAILURE,
                     f"stripe {stripe_index}: manifest fetch failed: {mval}")

    if mstate == "ok":
        leaves: list[bytes] = mval
        # Authenticate the manifest itself against the descriptor before
        # trusting any of its content-digest entries: a flipped manifest
        # byte is unverifiable metadata, not a "missing share".
        if stripe_root(stripe_index, leaves) \
                != desc.stripe_roots[stripe_index]:
            return _fail(result, ReadStatus.UNVERIFIABLE,
                         f"stripe {stripe_index}: manifest does not match "
                         f"descriptor stripe root")
        # Fast path: required range lies entirely in systematic shares.
        sys_idx = sorted(needed)
        authentic = await _fetch_authenticated(
            storage, desc, stripe_index, sys_idx, leaves, statuses, counter)
        fast_path_ok = needed <= set(authentic)
        if fast_path_ok:
            # Settle every non-needed position too, so callers and repair
            # get an accurate MISSING / AVAILABLE picture of the stripe.
            untouched = [j for j in range(desc.n)
                         if statuses[j] is ShareStatus.NOT_ATTEMPTED]
            extra = await _fetch_authenticated(
                storage, desc, stripe_index, untouched, leaves, statuses,
                counter,
                present_state=ShareStatus.AVAILABLE_NOT_NEEDED) \
                if untouched else {}
            for j in extra:
                statuses[j] = ShareStatus.AVAILABLE_NOT_NEEDED
            blocks_by_index = {j: authentic[j] for j in sys_idx}
            if not _check_tail_padding(desc, stripe_index, blocks_by_index):
                return _fail(result, ReadStatus.UNVERIFIABLE,
                             "stripe tail padding is non-zero")
            result.status = ReadStatus.CONFIRMED
            result.data_blocks = blocks_by_index
            result.used_share_indices = tuple(sys_idx)
            return result

        # Fast path failed (a needed systematic share was missing/corrupt).
        # Fetch every not-yet-judged position concurrently; already recorded
        # results (especially CORRUPT) are never overwritten.
        untouched = [j for j in range(desc.n)
                     if statuses[j] is ShareStatus.NOT_ATTEMPTED]
        if untouched:
            authentic.update(await _fetch_authenticated(
                storage, desc, stripe_index, untouched, leaves, statuses,
                counter))

        if len(authentic) < params.k:
            if any(s is ShareStatus.FETCH_FAILED for s in statuses.values()):
                return _fail(result, ReadStatus.STORAGE_FAILURE,
                             f"stripe {stripe_index}: storage failure while "
                             f"gathering shares")
            return _fail(result, ReadStatus.UNRECOVERABLE,
                         f"stripe {stripe_index}: only {len(authentic)} of "
                         f"{params.k} authentic shares available")

        chosen = sorted(authentic)[:params.k]
        try:
            decoded = coder.decode(
                params, [authentic[j] for j in chosen], chosen)
        except Exception as exc:  # noqa: BLE001
            return _fail(result, ReadStatus.UNVERIFIABLE,
                         f"stripe {stripe_index}: decoder rejected shares: "
                         f"{exc}")
        decoded_by_index = dict(enumerate(decoded))
        for j in range(params.k):
            if content_digest(decoded[j]) != leaves[j]:
                return _fail(result, ReadStatus.UNVERIFIABLE,
                             f"stripe {stripe_index}: reconstructed block "
                             f"{j} failed content verification")
        if not _check_tail_padding(desc, stripe_index, decoded_by_index):
            return _fail(result, ReadStatus.UNVERIFIABLE,
                         "stripe tail padding is non-zero")
        for j in chosen:
            statuses[j] = ShareStatus.USED
        # Authentic shares fetched but not needed for decoding: present on
        # storage, just redundant for this read.
        for j in authentic:
            if j not in chosen:
                statuses[j] = ShareStatus.AVAILABLE_NOT_NEEDED
        # Positions never fetched: settle MISSING vs present with a final
        # existence probe so repair writes back only what is truly absent.
        untouched = [j for j in range(desc.n)
                     if statuses[j] is ShareStatus.NOT_ATTEMPTED]
        if untouched:
            probe = await _fetch_authenticated(
                storage, desc, stripe_index, untouched, leaves, statuses,
                counter,
                present_state=ShareStatus.AVAILABLE_NOT_NEEDED)
            for j in probe:
                statuses[j] = ShareStatus.AVAILABLE_NOT_NEEDED
        result.status = ReadStatus.CONFIRMED
        result.data_blocks = decoded_by_index
        result.used_share_indices = tuple(chosen)
        return result

    # Manifest missing: repair/read recovery from raw blob keys is only
    # possible if some manifest-equivalent listing survives.  Without a
    # manifest there is no authenticated way to *name* the blob keys; we
    # report unrecoverable rather than scanning storage or trusting guesses.
    # (A write always publishes manifests before the descriptor, so this
    # state in practice means externally deleted metadata.)
    return _fail(result, ReadStatus.UNRECOVERABLE,
                 f"stripe {stripe_index}: manifest absent; cannot "
                 f"authenticate shares without it")


async def repair_stripe(storage, coder, desc: ObjectDescriptor,
                        stripe_index: int, *, replace_corrupt: bool,
                        repair_stats) -> RepairStripeResult:
    """Reconstruct and write back everything missing for one stripe.

    All writes target content-addressed ``blob`` keys via put-if-absent and
    the generation-scoped manifest key: meaning stays fixed by the
    descriptor, and a newer generation uses different manifest keys.
    """
    counter: dict[str, int] = {"fetches": 0, "bytes": 0}
    plan = await plan_stripe_read(storage, coder, desc, stripe_index,
                                  needed_data_blocks=None, counter=counter)
    read_to_repair = {
        ReadStatus.CONFIRMED: RepairStatus.REPAIRED,
        ReadStatus.UNRECOVERABLE: RepairStatus.UNRECOVERABLE,
        ReadStatus.UNVERIFIABLE: RepairStatus.UNVERIFIABLE,
        ReadStatus.STORAGE_FAILURE: RepairStatus.STORAGE_FAILURE,
        ReadStatus.CANCELLED: RepairStatus.CANCELLED,
    }
    res = RepairStripeResult(
        stripe_index=stripe_index,
        status=read_to_repair[plan.status],
        missing_before=plan.missing,
        corrupt_shares=plan.corrupt,
        manifest_regenerated=plan.manifest_regenerated,
        error=plan.error)
    if not plan.status.ok:
        return res

    # plan_stripe_read's fast path may have only systematic data; repair
    # needs all n blocks.
    data = [plan.data_blocks[j] for j in range(desc.k)]
    all_blocks = coder.encode(desc.params, data)
    leaves = [content_digest(all_blocks[j]) for j in range(desc.n)]
    if stripe_root(stripe_index, leaves) != desc.stripe_roots[stripe_index]:
        res.status = RepairStatus.UNVERIFIABLE
        res.error = (f"stripe {stripe_index}: reconstructed blocks do not "
                     f"satisfy descriptor stripe root")
        return res

    # Which shares are missing on storage?  Anything USED is present.
    missing = [j for j, st in plan.share_statuses.items()
               if st is ShareStatus.MISSING]
    corrupt = [j for j, st in plan.share_statuses.items()
               if st is ShareStatus.CORRUPT] if replace_corrupt else []
    to_write = sorted(set(missing + corrupt))

    async def write_share(j: int):
        block = all_blocks[j]
        created = await storage.put_if_absent(keys.blob_key(block), block)
        return j, created

    outcomes = await asyncio.gather(
        *(write_share(j) for j in to_write), return_exceptions=True)
    regenerated: list[int] = []
    already: list[int] = []
    for j, wr in zip(to_write, outcomes):
        if isinstance(wr, BaseException):
            res.status = RepairStatus.STORAGE_FAILURE
            res.error = f"repair write failed for share {j}: {wr}"
        elif wr[1]:
            regenerated.append(j)
            repair_stats.repair_bytes_written += desc.block_size
        else:
            already.append(j)

    # A missing manifest is restored from verified leaves (CAS); the
    # descriptor root above is what authenticates it.
    mstate, _ = await _get_manifest(storage, desc, stripe_index)
    if mstate == "missing":
        try:
            created = await storage.put_if_absent(
                keys.manifest_key(desc, stripe_index), manifest_blob(leaves))
            res.manifest_regenerated = bool(created)
            if created:
                repair_stats.manifests_regenerated += 1
        except Exception as exc:  # noqa: BLE001
            res.status = RepairStatus.STORAGE_FAILURE
            res.error = f"manifest write failed: {exc}"

    res.regenerated_shares = tuple(sorted(regenerated))
    res.already_present_shares = tuple(sorted(already))
    return res
