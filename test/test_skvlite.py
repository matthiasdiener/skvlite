import shutil
import sys  # noqa
import tempfile
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any

import pytest
from pytools.tag import Tag, tag_dataclass

from skvlite import KVStore as PersistentDict
from skvlite import ReadOnlyEntryError
from skvlite import WriteOnceKVStore as WriteOncePersistentDict


@tag_dataclass
class SomeTag(Tag):
    value: str


class MyEnum(Enum):
    YES = 1
    NO = 2


class MyIntEnum(IntEnum):
    YES = 1
    NO = 2


@dataclass
class MyStruct:
    name: str
    value: int


def test_persistent_dict_storage_and_lookup() -> None:
    try:
        tmpdir = tempfile.mkdtemp()
        pdict: PersistentDict[Any, Any] = \
            PersistentDict("pytools-test", container_dir=tmpdir)

        from random import randrange

        def rand_str(n: int = 20) -> str:
            return "".join(chr(65 + randrange(26)) for i in range(n))

        keys = [(randrange(2000) - 1000, rand_str(), None,
                 SomeTag(rand_str()),  # type: ignore[call-arg]
                 frozenset({"abc", 123}))
                for _ in range(20)]
        values = [randrange(2000) for i in range(20)]

        d = dict(zip(keys, values))

        # {{{ check lookup

        for k, v in zip(keys, values):
            pdict[k] = v

        for k, v in d.items():
            assert d[k] == pdict[k]
            assert v == pdict[k]

        # }}}

        # {{{ check updating

        for k, v in zip(keys, values):
            pdict[k] = v + 1

        for k, v in d.items():
            assert d[k] + 1 == pdict[k]
            assert v + 1 == pdict[k]

        # }}}

        # {{{ check store_if_not_present

        for k, _ in zip(keys, values):
            pdict.store_if_not_present(k, d[k] + 2)

        for k, v in d.items():
            assert d[k] + 1 == pdict[k]
            assert v + 1 == pdict[k]

        pdict.store_if_not_present(2001, 2001)
        assert pdict[2001] == 2001

        # }}}

        # {{{ check dataclasses

        for v in [17, 18]:
            key = MyStruct("hi", v)
            pdict[key] = v

            # reuse same key, with stored hash
            assert pdict[key] == v

        with pytest.raises(KeyError):
            pdict[MyStruct("hi", 19)]

        for v in [17, 18]:
            # make new key instances
            assert pdict[MyStruct("hi", v)] == v

        # }}}

        # {{{ check enums

        pdict[MyEnum.YES] = 1
        with pytest.raises(KeyError):
            pdict[MyEnum.NO]
        assert pdict[MyEnum.YES] == 1

        pdict[MyIntEnum.YES] = 12
        with pytest.raises(KeyError):
            pdict[MyIntEnum.NO]
        assert pdict[MyIntEnum.YES] == 12

        # }}}

        # check not found

        with pytest.raises(KeyError):
            pdict[3000]

    finally:
        shutil.rmtree(tmpdir)


def test_persistent_dict_deletion() -> None:
    try:
        tmpdir = tempfile.mkdtemp()
        pdict: PersistentDict[int, int] = \
            PersistentDict("pytools-test", container_dir=tmpdir)

        pdict[0] = 0
        del pdict[0]

        with pytest.raises(KeyError):
            pdict[0]

        with pytest.raises(KeyError):
            del pdict[1]

    finally:
        shutil.rmtree(tmpdir)


def test_persistent_dict_synchronization() -> None:
    try:
        tmpdir = tempfile.mkdtemp()
        pdict1: PersistentDict[int, int] = \
            PersistentDict("pytools-test", container_dir=tmpdir)
        pdict2: PersistentDict[int, int] = \
            PersistentDict("pytools-test", container_dir=tmpdir)

        # check lookup
        pdict1[0] = 1
        assert pdict2[0] == 1

        # check updating
        pdict1[0] = 2
        assert pdict2[0] == 2

        # check deletion
        del pdict1[0]
        with pytest.raises(KeyError):
            pdict2[0]

    finally:
        shutil.rmtree(tmpdir)


def test_persistent_dict_clear() -> None:
    try:
        tmpdir = tempfile.mkdtemp()
        pdict: PersistentDict[int, int] = \
            PersistentDict("pytools-test", container_dir=tmpdir)

        pdict[0] = 1
        pdict[0]
        pdict.clear()

        with pytest.raises(KeyError):
            pdict[0]

    finally:
        shutil.rmtree(tmpdir)


def test_write_once_persistent_dict_storage_and_lookup() -> None:
    try:
        tmpdir = tempfile.mkdtemp()
        pdict: WriteOncePersistentDict[int, int] = \
            WriteOncePersistentDict("pytools-test", container_dir=tmpdir)

        # check lookup
        pdict[0] = 1
        assert pdict[0] == 1
        # do two lookups to test the cache
        assert pdict[0] == 1

        # check updating
        with pytest.raises(ReadOnlyEntryError):
            pdict[0] = 2

        # check not found
        with pytest.raises(KeyError):
            pdict[1]

    finally:
        shutil.rmtree(tmpdir)


def test_write_once_persistent_dict_synchronization() -> None:
    try:
        tmpdir = tempfile.mkdtemp()
        pdict1: WriteOncePersistentDict[int, int] = \
            WriteOncePersistentDict("pytools-test", container_dir=tmpdir)
        pdict2: WriteOncePersistentDict[int, int] = \
            WriteOncePersistentDict("pytools-test", container_dir=tmpdir)

        # check lookup
        pdict1[1] = 0
        assert pdict2[1] == 0

        # check updating
        with pytest.raises(ReadOnlyEntryError):
            pdict2[1] = 1

    finally:
        shutil.rmtree(tmpdir)


def test_write_once_persistent_dict_clear() -> None:
    try:
        tmpdir = tempfile.mkdtemp()
        pdict: WriteOncePersistentDict[int, int] = \
            WriteOncePersistentDict("pytools-test", container_dir=tmpdir)

        pdict[0] = 1
        pdict[0]
        assert 0 in pdict

        pdict.clear()

        with pytest.raises(KeyError):
            pdict[0]

    finally:
        shutil.rmtree(tmpdir)


def test_speed() -> None:
    import time

    tmpdir = tempfile.mkdtemp()
    pdict: WriteOncePersistentDict[int, int] = \
        WriteOncePersistentDict("pytools-test", container_dir=tmpdir)

    start = time.time()
    for i in range(10000):
        pdict[i] = i
    end = time.time()
    print("persistent dict write time: ", end - start)

    start = time.time()
    for _ in range(5):
        for i in range(10000):
            pdict[i]
    end = time.time()
    print("persistent dict read time: ", end - start)

    shutil.rmtree(tmpdir)


def test_size() -> None:
    try:
        tmpdir = tempfile.mkdtemp()
        pdict: PersistentDict[str, int] = \
            PersistentDict("pytools-test", container_dir=tmpdir)

        for i in range(10000):
            pdict[f"{i}{i}{i}{i}{i}{i}{i}"] = i

        size = pdict.nbytes()
        print("sqlite size: ", size / 1024 / 1024, " MByte")
        assert 1 * 1024 * 1024 < size < 2 * 1024 * 1024
    finally:
        shutil.rmtree(tmpdir)


def test_len() -> None:
    try:
        tmpdir = tempfile.mkdtemp()
        pdict: PersistentDict[int, int] = \
            PersistentDict("pytools-test", container_dir=tmpdir)

        assert len(pdict) == 0

        for i in range(10000):
            pdict[i] = i

        assert len(pdict) == 10000

        pdict.clear()

        assert len(pdict) == 0
    finally:
        shutil.rmtree(tmpdir)


def test_repr() -> None:
    try:
        tmpdir = tempfile.mkdtemp()
        pdict: PersistentDict[Any, Any] = \
            PersistentDict("pytools-test", container_dir=tmpdir)

        assert repr(pdict)[:8] == "KVStore("
    finally:
        shutil.rmtree(tmpdir)


def test_keys_values_items() -> None:
    try:
        tmpdir = tempfile.mkdtemp()
        pdict: PersistentDict[int, int] = \
            PersistentDict("pytools-test", container_dir=tmpdir)

        for i in range(10000):
            pdict[i] = i

        # This also tests deterministic iteration order
        assert len(list(pdict.keys())) == 10000 == len(set(pdict.keys()))
        assert list(pdict.keys()) == list(range(10000))
        assert list(pdict.values()) == list(range(10000))
        assert list(pdict.items()) == list(zip(list(pdict.keys()), range(10000)))

        assert ([k for k in pdict.keys()]  # noqa: C416
                == list(pdict.keys())
                == list(pdict)
                == [k for k in pdict])  # noqa: C416

    finally:
        shutil.rmtree(tmpdir)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        exec(sys.argv[1])
    else:
        pytest.main([__file__])
