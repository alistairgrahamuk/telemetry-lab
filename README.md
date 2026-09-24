# telemetry-lab

A concurrent event-processing pipeline in Go: thousands of simulated temperature
sensors, a worker pool, windowed aggregation and threshold alarms — in a single
binary, with no dependencies outside the standard library.

Sustains **~100,000 events/sec** from 10,000 simulated devices on a 2-core machine,
race-detector clean.

```
10024 events/sec
ALARM: device 428 above threshold -14.81 at 2026-09-22T16:28:14+01:00
CLEARED: device 428 below threshold -15.54 at 2026-09-22T16:28:16+01:00
--------------------------------------------------
Devices: 1000, Readings: 50019, Active Alarms: 1
Min: -22.14, Max: -13.10, Avg: -19.27
```

## What it does

Each simulated device is a goroutine with its own ticker and its own temperature,
drifting around a baseline with the occasional "door left open" excursion. Readings
flow over a channel to a pool of workers, which maintain per-device statistics and a
sustained-breach alarm state machine behind a mutex. Every few seconds the fleet is
aggregated into a summary line. Ctrl-C shuts everything down in order, losing nothing
in flight.

## Requirements

Go 1.22 or later (it uses range-over-int and the `min`/`max` builtins). No modules to
fetch.

## Running it

```bash
go run .                                     # 2 devices, 1 reading/sec each
go run . -devices=1000 -rate=10              # 10,000 events/sec
go run . -devices=10000 -rate=10 -workers=8  # ~100,000 events/sec
go run -race . -devices=1000 -rate=10        # under the race detector
```

Or build a static binary:

```bash
go build -o telemetry-lab .
./telemetry-lab -devices=5000 -rate=20 -window=10s
```

Press Ctrl-C to stop. It prints a final partial-window summary and exits 0.

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `-devices` | `2` | Simulated devices, one goroutine each |
| `-rate` | `1` | Readings per second, **per device** |
| `-workers` | `NumCPU()` | Goroutines consuming the readings channel |
| `-window` | `5s` | Aggregation window for the fleet summary |

Total offered load is `devices × rate`. Bad input exits with status 2 and a message.

## How it works

```
N device goroutines ──▶ chan reading ──▶ W workers ──▶ store (mutex) ──▶ reporter
```

- **Devices** own their state exclusively — one goroutine each, no locking.
- **The channel** is the only handover between producers and consumers, unbuffered,
  so a slow consumer applies backpressure directly.
- **The store** is the one piece of shared mutable state, reached only through methods
  that take its mutex.
- **`main`** is the reporter: events/sec every second, a fleet summary every window,
  and a final summary on shutdown.

[`ARCHITECTURE.md`](../docs/ARCHITECTURE.md) has the full design: goroutine inventory, data
flow, every type and function, the concurrency model, the shutdown sequence and the
known limitations.

## Verifying

```bash
go vet ./...
go run -race . -devices=1000 -rate=10
```

The race detector exits with status 66 if it finds anything.

## Measured throughput

On a 2-core machine:

| Configuration | Sustained |
|---|---|
| 10,000 devices × 10/sec | ~100,000/sec |
| 10,000 devices × 100/sec | ~590,000/sec (against a 1,000,000/sec target) |
| 1 device × 1,000,000/sec | ~100,000/sec |

Throughput comes from many devices rather than fast ones: a single goroutine can't
service a 1 µs ticker, and Go's tickers drop missed ticks rather than queueing them.

## Status and next steps

Working, and deliberately small. Known limitations are listed in full in
[`ARCHITECTURE.md`](../docs/ARCHITECTURE.md); the ones worth fixing first:

- **One global mutex** serialises every reading — shard the store by device ID.
- **Ordering isn't guaranteed** across workers, so "consecutive readings" is
  approximate. Partitioning devices across workers would make it exact.
- **No hysteresis** on alarms, so a device hovering at the threshold flaps.
- **Single file**, with the threshold and sustain count as compile-time constants.

## Licence

MIT — see [LICENSE](LICENSE).
