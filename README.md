# framed

Fast and predictable networking library.

### Features
- tcp framing
- zero allocations after startup
- highly configurable (max_packet_size, max_packet_backlog, max_packets_per_tick, prefix len and endianness)
- event driven (connected, disconnected and packet_received)
- stable and predictable (soak test can run for hours with no change on cpu or memory usage)
- low memory footprint
- single threaded: no threads, no sync, no bullshit, io must be dealt with by the os, not with threads
- official c abi support

### Load Test - 1000 Clients

**Server**

| Benchmark |      CPU |      Memory |   Throughput In/Out |   Messages/s |
| --------- | -------: | ----------: | ------------------: | -----------: |
| enet      | **4.9%** | **14.1 MB** |         7.594 MiB/s |      165,896 |
| telepathy |    21.1% |     93.5 MB |         5.822 MiB/s |      127,196 |
| framed    | **4.9%** |     34.2 MB |    **12.138 MiB/s** |  **265,124** |


**Client**

| Benchmark |      CPU |     Memory |   Throughput In/Out |   Messages/s |
| --------- | -------: | ---------: | ------------------: | -----------: |
| enet      | **4.8%** | **1.7 MB** |         7.588 MiB/s |      165,820 |
| telepathy |    18.3% |    88.2 MB |         5.822 MiB/s |      127,191 |
| framed    |     5.4% |    33.0 MB |    **12.134 MiB/s** |  **265,054** |


Notes:
- ENet is running under unfavorable conditions (single channel and reliable packets).
- Telepathy required a `Thread.Sleep(1)` in the client loop to prevent crashing. Both ENet and Framed required no equivalent and remained stable under the same stress test.
