# Resume entry

## Distributed In-Memory Data Engine | C++20

- Engineered a thread-safe TCP caching service with expected O(1) key lookup and LRU eviction using `std::unordered_map`, `std::list`, stored iterators, and synchronized multi-client access.
- Implemented per-key TTL expiration, checksummed append-only persistence, snapshot recovery, channel Pub/Sub, runtime statistics, and a quoted protocol supporting values with spaces.
- Built deterministic consistent-hash routing with 128 virtual nodes per server, static three-node configuration, ring/key inspection, and explicit unavailable-owner reporting.
- Created CMake builds, GoogleTest and integration coverage, Docker Compose deployment, and a repeatable in-process benchmark reporting throughput and average/P50/P95/P99 latency; the verified suite passed 6/6 CTest entries.

**Technical skills:** C++20, data structures, LRU caching, concurrency, mutexes and atomics, TCP/Winsock/POSIX sockets, binary persistence, failure recovery, consistent hashing, CMake, GoogleTest, Docker Compose, PowerShell/Bash, GitHub Actions, performance measurement.

**Engineering complexity:** The engine coordinates map/list invariants, expiration metadata, statistics, persistence ordering, socket lifecycles, and asynchronous Pub/Sub delivery across concurrent clients. It also separates partition ownership from availability: a failed node is detected and reported rather than unsafely rerouting unreplicated data.

Developer: Shivansh Kumar  
Portfolio: https://shivanshonline.in  
GitHub: https://github.com/shivansh07adi-cloud
