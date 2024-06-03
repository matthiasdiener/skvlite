# Run this with 'mpirun -n 8 python concurrency.py'

import time

from mpi4py import MPI

from skvlite import (KVStore, NoSuchEntryError, ReadOnlyEntryError,
                     WriteOnceKVStore)

N = 10000

mydir = "./tmp"
pdict = KVStore("skvlite.sqlite", container_dir=mydir)
rank = MPI.COMM_WORLD.Get_rank()

s = 0

start = time.time()

for i in range(N):
    if i % 1000 == 0:
        print(f"rank={rank} i={i}")
    pdict[i] = i

    try:
        s += pdict[i]
    except NoSuchEntryError:
        # Someone else already deleted the entry
        pass

    try:
        del pdict[i]
    except NoSuchEntryError:
        # Someone else already deleted the entry
        pass

end = time.time()

print(f"rank={rank} KVStore: time taken to write {N} entries to {mydir}/skvlite.sqlite: {end-start} s={s}")

pdict = WriteOnceKVStore("skvlite_writeonce.sqlite", container_dir=mydir)

s = 0

start = time.time()

for i in range(N):
    if i % 1000 == 0:
        print(f"rank={rank} i={i}")

    try:
        pdict[i] = i
    except ReadOnlyEntryError:
        # Someone else already added the entry
        pass

    s += pdict[i]

end = time.time()

print(f"rank={rank} WriteOnceKVStore: time taken to write {N} entries to {mydir}/skvlite.sqlite: {end-start} s={s}")
