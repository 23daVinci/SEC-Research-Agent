import os
from pathlib import Path

import pytest

from sec_ingest.blobstore import ImmutabilityViolation, LocalBlobStore


@pytest.fixture
def store(tmp_path: Path) -> LocalBlobStore:
    return LocalBlobStore(tmp_path)


def test_roundtrip_and_exists(store: LocalBlobStore) -> None:
    assert not store.exists("a/b.txt")
    store.put_bytes("a/b.txt", b"hello")
    assert store.exists("a/b.txt")
    assert store.get_bytes("a/b.txt") == b"hello"


def test_same_bytes_is_noop(store: LocalBlobStore) -> None:
    store.put_bytes("k", b"x")
    store.put_bytes("k", b"x")
    assert store.get_bytes("k") == b"x"


def test_different_bytes_raises(store: LocalBlobStore) -> None:
    store.put_bytes("k", b"x")
    with pytest.raises(ImmutabilityViolation):
        store.put_bytes("k", b"y")
    assert store.get_bytes("k") == b"x"


def test_explicit_overwrite_allowed(store: LocalBlobStore) -> None:
    store.put_bytes("k", b"x")
    store.put_bytes("k", b"y", overwrite=True)
    assert store.get_bytes("k") == b"y"


def test_crash_between_temp_write_and_replace_leaves_no_partial(
    store: LocalBlobStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    store.put_bytes("k", b"old")

    def boom(*_: object) -> None:
        raise OSError("simulated crash")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError, match="simulated"):
        store.put_bytes("k", b"new", overwrite=True)
    monkeypatch.undo()

    assert store.get_bytes("k") == b"old"
    assert list(store.list("")) == ["k"]  # temp file cleaned up and never listed


@pytest.mark.parametrize("key", ["", "/abs", "../escape", "a/../../b", "a\\b"])
def test_rejects_unsafe_keys(store: LocalBlobStore, key: str) -> None:
    with pytest.raises(ValueError):
        store.put_bytes(key, b"x")


def test_list_prefix_is_sorted_and_relative(store: LocalBlobStore) -> None:
    store.put_bytes("f/b", b"1")
    store.put_bytes("f/a", b"1")
    store.put_bytes("g/c", b"1")
    assert list(store.list("f")) == ["f/a", "f/b"]
    assert list(store.list("missing")) == []


def test_append_line(store: LocalBlobStore) -> None:
    store.append_line("log.jsonl", "one")
    store.append_line("log.jsonl", "two")
    assert store.get_bytes("log.jsonl") == b"one\ntwo\n"
