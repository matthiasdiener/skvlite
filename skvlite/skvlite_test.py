import tempfile
import shutil
import time
from skvlite import WriteOnceKVStore as WriteOncePersistentDict, KVStore as PersistentDict

def test_speed():
    tmpdir = tempfile.mkdtemp()
    pdict = WriteOncePersistentDict("pytools-test", container_dir=tmpdir)

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

def test_size():
    try:
        tmpdir = tempfile.mkdtemp()
        pdict = PersistentDict("pytools-test", container_dir=tmpdir)

        for i in range(10000):
            pdict[f"foobarbazfoobbb{i}"] = i

        size = pdict.size()
        print("sqlite size: ", size / 1024 / 1024, " MByte")
        assert 1 * 1024 * 1024 < size < 2 * 1024 * 1024
    finally:
        shutil.rmtree(tmpdir)

if __name__ == "__main__":
    test_speed()
    test_size()
