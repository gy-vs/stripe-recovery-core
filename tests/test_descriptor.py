"""Descriptor persistence and tamper-evidence."""

import dataclasses

import pytest

from stripe_recovery_core import DescriptorError, ObjectDescriptor
from util import make_kernel, pattern_bytes, run


def _put_descriptor():
    async def go():
        kernel, _ = make_kernel()
        result = await kernel.put("obj", pattern_bytes(1000))
        return result.descriptor

    return run(go())


def test_descriptor_bytes_roundtrip():
    desc = _put_descriptor()
    raw = desc.to_bytes()
    again = ObjectDescriptor.from_bytes(raw)
    assert again == desc
    assert again.content_sha256 == desc.content_sha256
    assert again.generation == desc.generation


def test_descriptor_envelope_detects_corruption():
    desc = _put_descriptor()
    raw = bytearray(desc.to_bytes())
    raw[len(raw) // 2] ^= 0x01
    with pytest.raises(DescriptorError):
        ObjectDescriptor.from_bytes(bytes(raw))


def test_descriptor_rejects_tampered_stripe_hash():
    desc = _put_descriptor()
    stripe0 = desc.stripes[0]
    tampered_stripe = dataclasses.replace(stripe0, sha256="00" * 32)
    data = desc.to_dict()
    data["stripes"][0] = tampered_stripe.to_dict()
    with pytest.raises(DescriptorError):
        # manifest root no longer matches the stripe list
        ObjectDescriptor.from_dict(data)


def test_descriptor_rejects_inconsistent_length():
    desc = _put_descriptor()
    data = desc.to_dict()
    data["length"] = desc.length + 1
    with pytest.raises(DescriptorError):
        ObjectDescriptor.from_dict(data)


def test_descriptor_rejects_foreign_format():
    desc = _put_descriptor()
    data = desc.to_dict()
    data["format"] = "something-else/9"
    with pytest.raises(DescriptorError):
        ObjectDescriptor.from_dict(data)


def test_stripe_index_ranges():
    desc = _put_descriptor()  # 1000 bytes, 256-byte stripes -> 4 stripes
    assert desc.stripe_count == 4
    assert list(desc.stripe_indices_for(0, 1)) == [0]
    assert list(desc.stripe_indices_for(255, 2)) == [0, 1]
    assert list(desc.stripe_indices_for(100, 900)) == [0, 1, 2, 3]
    assert list(desc.stripe_indices_for(512, 0)) == []
