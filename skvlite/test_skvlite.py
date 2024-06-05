import tempfile
import shutil
import pytest
from skvlite import KVStore

def test_speed():
    import time

    tmpdir = tempfile.mkdtemp()
    pdict = KVStore("pytools-test", container_dir=tmpdir)

    start = time.time()
    for i in range(10000):
        pdict[i] = i
    end = time.time()
    print("persistent dict write time: ", end-start)

    start = time.time()
    for _ in range(5):
        for i in range(10000):
            pdict[i]
    end = time.time()
    print("persistent dict read time: ", end-start)

    shutil.rmtree(tmpdir)

if __name__ == "__main__":
    pytest.main(["-s", "-v"])

def test_size():
    try:
        tmpdir = tempfile.mkdtemp()
        pdict = KVStore("pytools-test", container_dir=tmpdir)

        for i in range(10000):
            pdict[f"foobarbazfoobbb{i}"] = i

        size = pdict.size()
        print("sqlite size: ", size/1024/1024, " MByte")
        assert 1*1024*1024 < size < 2*1024*1024
    finally:
        shutil.rmtree(tmpdir)
