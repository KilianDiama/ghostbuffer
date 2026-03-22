⚡ Engineered by Kiliandiama | The Diama Protocol [10/10] | All rights reserved.
GhostBuffer 🫧

GhostBuffer is a high-performance shared memory buffer for inter-process communication (IPC) in Python. It uses cache-line aligned slots, atomic C operations for synchronization, and a lightweight backpressure mechanism to maximize throughput.

🚀 Features

High-performance IPC via multiprocessing.shared_memory.

Lock-free writing and reading using atomic operations (CAS, atomic_add).

Cache-line alignment to avoid CPU false sharing.

Backpressure support to prevent buffer overflow.

Multi-slot buffer with power-of-two slot count.

Timestamping with time.monotonic_ns() for approximate real-time tracking.

Linux/macOS compatible (requires gcc to compile the atomic C module).
