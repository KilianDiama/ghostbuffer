import ctypes
import os
import sys
import subprocess
import time
from multiprocessing import shared_memory
from typing import Callable, Optional

# --- CONSTANTS ---
CACHE_LINE = 64
SLOT_SIZE = 65536
STATUS_FREE, STATUS_WRITING, STATUS_READY, STATUS_READING = 0, 1, 2, 3

# --- DYNAMIC C KERNEL FOR ATOMICS ---
C_CODE = """
#include <stdint.h>
#include <stdbool.h>

void memory_barrier() { __sync_synchronize(); }
int64_t atomic_add(int64_t* ptr, int64_t val) { return __sync_fetch_and_add(ptr, val); }
bool atomic_cas(int32_t* ptr, int32_t exp, int32_t des) { return __sync_bool_compare_and_swap(ptr, exp, des); }
void cpu_pause() { __asm__ __volatile__ ("pause" ::: "memory"); }
"""

def load_atomic_lib() -> ctypes.CDLL:
    """Compile and load the atomic C library."""
    lib_path = "./libghost_atomic.so"
    if not os.path.exists(lib_path):
        with open("ghost_atomic.c", "w") as f:
            f.write(C_CODE)
        subprocess.run(
            ["gcc", "-O3", "-shared", "-o", lib_path, "-fPIC", "ghost_atomic.c"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    lib = ctypes.CDLL(lib_path)
    lib.atomic_add.argtypes = [ctypes.POINTER(ctypes.c_int64), ctypes.c_int64]
    lib.atomic_add.restype = ctypes.c_int64
    lib.atomic_cas.argtypes = [ctypes.POINTER(ctypes.c_int32), ctypes.c_int32, ctypes.c_int32]
    lib.atomic_cas.restype = ctypes.c_bool
    return lib

# --- SLOT STRUCTURE ---
class FastSlot(ctypes.Structure):
    _fields_ = [
        ("status", ctypes.c_int32),
        ("length", ctypes.c_uint32),
        ("ts", ctypes.c_int64),
        ("data", ctypes.c_ubyte * SLOT_SIZE)
    ]
    _align_ = CACHE_LINE

# --- HIGH-PERFORMANCE IPC BUFFER ---
class GhostBuffer:
    def __init__(self, name: str, is_host: bool = False, slot_count: int = 1024):
        if slot_count & (slot_count - 1) != 0:
            raise ValueError("slot_count must be a power of two")
        
        self.lib = load_atomic_lib()
        self.slot_count = slot_count
        self.mask = slot_count - 1
        self.slot_size = ctypes.sizeof(FastSlot)
        self.ctrl_section = CACHE_LINE * 32  # control region
        total_size = self.ctrl_section + (self.slot_size * slot_count)

        self.is_host = is_host
        self.shm_name = name
        self._closed = False

        if is_host:
            try: shared_memory.SharedMemory(name=name).unlink()
            except FileNotFoundError: pass
            self.shm = shared_memory.SharedMemory(create=True, size=total_size, name=name)
            ctypes.memset(self.shm.buf, 0, total_size)
        else:
            self.shm = shared_memory.SharedMemory(name=name)

        self.base_addr = ctypes.addressof(ctypes.c_char.from_buffer(self.shm.buf))
        self.head_ptr = ctypes.cast(self.base_addr, ctypes.POINTER(ctypes.c_int64))
        self.tail_ptr = ctypes.cast(self.base_addr + (CACHE_LINE * 8), ctypes.POINTER(ctypes.c_int64))
        self._cached_tail = 0

    def _get_slot(self, idx: int) -> FastSlot:
        return FastSlot.from_address(self.base_addr + self.ctrl_section + (idx * self.slot_size))

    def write(self, data: bytes, timeout: Optional[float] = None) -> bool:
        """Write a message to the buffer with optional timeout."""
        if self._closed:
            raise RuntimeError("GhostBuffer is closed")
        if len(data) > SLOT_SIZE:
            return False

        start_time = time.monotonic()
        ticket = self.lib.atomic_add(self.head_ptr, 1)

        # backpressure
        if ticket - self._cached_tail >= self.slot_count:
            self._cached_tail = self.tail_ptr.contents.value
            if ticket - self._cached_tail >= self.slot_count:
                return False

        slot = self._get_slot(ticket & self.mask)
        spins = 0
        backoff = 1e-7

        while not self.lib.atomic_cas(ctypes.byref(slot.status), STATUS_FREE, STATUS_WRITING):
            self.lib.cpu_pause()
            spins += 1
            if spins > 1000:
                time.sleep(backoff)
                backoff = min(backoff * 2, 0.001)
            if timeout and (time.monotonic() - start_time) > timeout:
                return False

        # copy data
        ctypes.memmove(slot.data, data, len(data))
        slot.length = len(data)
        slot.ts = time.monotonic_ns()
        self.lib.memory_barrier()
        slot.status = STATUS_READY
        return True

    def consume(self, callback: Callable[[memoryview], None], timeout: Optional[float] = None):
        """Consume messages from the buffer, calling `callback` on each."""
        if self._closed:
            raise RuntimeError("GhostBuffer is closed")
        start_time = time.monotonic()

        try:
            while True:
                curr_tail = self.tail_ptr.contents.value
                slot = self._get_slot(curr_tail & self.mask)

                if slot.status != STATUS_READY:
                    if timeout and (time.monotonic() - start_time) > timeout:
                        return
                    time.sleep(0)
                    continue

                if self.lib.atomic_cas(ctypes.byref(slot.status), STATUS_READY, STATUS_READING):
                    callback(memoryview(slot.data)[:slot.length])
                    slot.status = STATUS_FREE
                    self.lib.memory_barrier()
                    self.lib.atomic_add(self.tail_ptr, 1)
        except KeyboardInterrupt:
            pass

    def close(self):
        """Close the buffer and release shared memory."""
        if not self._closed:
            try: self.shm.close()
            except Exception: pass
            if self.is_host:
                try: self.shm.unlink()
                except FileNotFoundError: pass
            self._closed = True

# --- EXAMPLE USAGE ---
if __name__ == "__main__":
    buf_name = "ghost_shm"
    if len(sys.argv) > 1 and sys.argv[1] == "host":
        print("🚀 GHOST HOST STARTED")
        buf = GhostBuffer(buf_name, is_host=True)
        msg = b"DATA_" * 100
        try:
            while True:
                if not buf.write(msg):
                    time.sleep(0.001)
        except KeyboardInterrupt:
            buf.close()
            print("Host stopped.")
    else:
        print("👻 GHOST CLIENT STARTED")
        buf = GhostBuffer(buf_name, is_host=False)
        def on_data(mv): pass  # ultra-fast processing
        try:
            buf.consume(on_data)
        except KeyboardInterrupt:
            buf.close()
            print("Client stopped.")
