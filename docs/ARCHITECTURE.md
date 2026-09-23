# telemetry-lab

A single-binary simulation of a fleet of temperature sensors and the pipeline that
processes their readings. It exists to exercise Go's concurrency primitives —
goroutines, channels, `context`, mutexes and atomics — under a realistic
event-processing workload, and to measure how much throughput a single process can
sustain.

Everything runs in one process. There is no network, no database and no external
dependency beyond the standard library.

---

## Quick start

```bash
go run .                                        # 2 devices, 1 reading/sec each
go run . -devices=1000 -rate=10                 # 10,000 events/sec
go run . -devices=10000 -rate=10 -workers=8     # ~100,000 events/sec
go run -race . -devices=1000 -rate=10           # with the race detector
```

Press **Ctrl-C** to stop. The program prints a final partial-window summary and exits
with status 0.

Output looks like this:

```
Number of devices: 1000 Readings per device per second: 10
Creating a new devices...
10024 events/sec
10136 events/sec
ALARM: device 428 above threshold -14.81 at 2026-09-22T16:28:14+01:00
...
--------------------------------------------------
Devices: 1000, Readings: 50019, Active Alarms: 1
Min: -22.14, Max: -13.10, Avg: -19.27
```

---

## Flags

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `-devices` | int | `2` | Number of simulated devices. One goroutine each. |
| `-rate` | int | `1` | Readings per second **per device**. Total offered load is `devices × rate`. |
| `-workers` | int | `runtime.NumCPU()` | Goroutines consuming the readings channel. |
| `-window` | duration | `5s` | Aggregation window for the fleet summary. Accepts `5s`, `500ms`, `1m`. |

Validation runs immediately after `flag.Parse()`, before any setup. Invalid input
writes a message to stderr and exits with status **2**:

- `devices < 1`
- `rate < 1` or `rate > 1_000_000`
- `workers < 1`
- `window <= 0`

The bounds are not arbitrary. `interval := time.Second / time.Duration(*rate)` divides
by zero at `rate = 0`, and produces a zero or negative interval outside these bounds,
which makes `time.NewTicker` panic inside a device goroutine — and a panic in any
goroutine terminates the whole process.

---

## Architecture

```
  ┌──────────────┐   ┌──────────────┐          ┌──────────────┐
  │ device 0     │   │ device 1     │   ...    │ device N-1   │   N goroutines
  │ ticker+state │   │ ticker+state │          │ ticker+state │
  └──────┬───────┘   └──────┬───────┘          └──────┬───────┘
         │                  │                         │
         └──────────────────┼─────────────────────────┘
                            ▼
                  chan reading (unbuffered)
                            │
         ┌──────────────────┼──────────────────┐
         ▼                  ▼                  ▼
  ┌────────────┐     ┌────────────┐     ┌────────────┐
  │ worker 0   │     │ worker 1   │ ... │ worker W-1 │          W goroutines
  └──────┬─────┘     └──────┬─────┘     └──────┬─────┘
         │                  │                  │
         └──────────────────┼──────────────────┘
                            ▼
                    ┌───────────────┐
                    │  store        │   sync.Mutex + map[int]*deviceStats
                    └───────┬───────┘
                            │ snapshot() every -window
                            ▼
                    ┌───────────────┐
                    │  main         │   reporter: 1s rate + window summary
                    └───────────────┘
```

Three tiers, each with its own concurrency rule:

1. **Devices** own their state exclusively. No sharing, so no locks.
2. **The channel** is the only handover point between producers and consumers.
3. **The store** is shared by every worker and by the reporter, so all access goes
   through one mutex.

### Goroutine inventory

| Goroutine | Count | Started by | Exits when |
|---|---|---|---|
| device | `-devices` | `main`'s start loop | `ctx` is cancelled |
| closer | 1 | `main` | after `wg.Wait()` returns; closes `readings` |
| worker | `-workers` | `main`'s worker loop | `readings` is closed and drained |
| done-signaller | 1 | `main` | after `worksWG.Wait()`; closes `done` |
| reporter | — | runs in `main` itself | `done` is closed |

`main` is the reporter, so it blocks in the `select` loop for the lifetime of the
program rather than returning early and killing everything.

---

## Data flow

1. A device's ticker fires every `interval`.
2. `next()` advances that device's temperature and returns the new value.
3. The device builds a `reading{deviceID, value, ts}` and sends it on the channel,
   in a `select` that also watches `ctx.Done()` so it can never block forever.
4. A worker receives it, increments the atomic event counter, and calls `store.add`.
5. `add` updates that device's stats under the lock and returns an `alarmEvent`.
6. The worker prints an alarm line if the event is `raised` or `cleared` — **after**
   the lock has been released.
7. Every `-window`, the reporter calls `store.snapshot()`, which aggregates the fleet,
   resets the per-window counters, and returns a `summary` to print.

---

## Type reference

### `device`

```go
type device struct {
    id          int
    baseline    float64 // the temperature this device settles at
    temperature float64 // current temperature
    excursion   int     // ticks of "door open" warming remaining
}
```

Owned by exactly one goroutine for its entire life. Never shared, never locked.
Created by `createDevice`, driven by `run`, mutated only by `next`.

### `reading`

```go
type reading struct {
    deviceID int
    value    float64
    ts       time.Time
}
```

An immutable value copied onto the channel. Because it is a plain struct with no
pointers or slices, sending it transfers a copy and there is no shared memory behind
it — which is why no reading ever needs a lock.

### `deviceStats`

```go
type deviceStats struct {
    count          int     // readings this window
    min, max, sum  float64 // this window
    aboveThreshold int     // consecutive readings over threshold
    alarm          bool    // alarm currently active
}
```

Two groups of fields with different lifetimes:

- **Window fields** (`count`, `min`, `max`, `sum`) are reset by every `snapshot()`.
- **Alarm fields** (`aboveThreshold`, `alarm`) persist across windows, because an
  alarm can span several of them.

Always held as `*deviceStats` in the map. Storing values instead of pointers would
mean `range` hands out copies, and the resets in `snapshot` would silently do nothing.

### `alarmEvent`

```go
type alarmEvent int

const (
    noEvent alarmEvent = iota
    raised
    cleared
)
```

Returned by `add` so that the *decision* (made under the lock) is separated from the
*reporting* (done without it). `iota` numbers the constants 0, 1, 2.

### `store`

```go
type store struct {
    mu      sync.Mutex
    devices map[int]*deviceStats
}
```

The only shared mutable state in the program. The mutex and the data it protects are
declared together, and the map is unexported with no accessor, so there is no way to
reach the data without going through a method that takes the lock.

Must always be used as `*store`: copying a struct containing a `sync.Mutex` copies
the lock and breaks mutual exclusion. `go vet` reports this.

### `summary`

```go
type summary struct {
    devices, readings, activeAlarms int
    min, max, avg                   float64
}
```

An immutable snapshot of one window, returned by value so the caller can print it
without holding the lock.

---

## Function reference

### `main()`

Ordering matters here:

1. Parse and validate flags.
2. Create `ctx` via `signal.NotifyContext(context.Background(), os.Interrupt)`, so
   Ctrl-C cancels the context rather than killing the process.
3. Create the unbuffered `readings` channel.
4. Start `-devices` device goroutines, tracked by `wg`.
5. Start the closer goroutine: `wg.Wait()` then `close(readings)`.
6. Start `-workers` worker goroutines, tracked by `worksWG`.
7. Start the done-signaller: `worksWG.Wait()` then `close(done)`.
8. Run the reporter loop.

Steps 5 and 7 are the pattern for "wait for a group without blocking": a `WaitGroup`
can only be waited on by blocking, so the wait happens in its own goroutine and
converts into a channel close, which `select` can watch.

### `createDevice(id int) device`

Returns a device with a baseline drawn from `-20 + rand.Float64()*1.5`
(a uniform −20 to −18.5 °C) and its temperature starting at that baseline.

### `(*device) next() float64`

Advances the simulation by one tick and returns the new temperature:

```go
if d.excursion == 0 && rand.Float64() < 0.00005 { d.excursion = 40 }
if d.excursion > 0 { d.temperature += 0.3; d.excursion-- }

noise := rand.NormFloat64() * 0.2
pull  := (d.baseline - d.temperature) * 0.05
d.temperature += noise + pull
```

The model is a first-order autoregressive process: each tick adds Gaussian noise and
pulls 5% of the way back towards the baseline. That gives a stationary standard
deviation of roughly 0.64 °C, so normal operation stays within about ±2 °C of the
baseline.

An **excursion** models a door left open: a 0.005% chance per tick starts 40 ticks of
+0.3 °C warming, about +12 °C of push. The 5% pull fights it, so the temperature peaks
somewhere around −14 to −15 °C and then recovers. This is the only mechanism that can
cross the alarm threshold; without it, −15 °C is more than five standard deviations
away from even the warmest device and would never be reached.

Pointer receiver, because it mutates the device.

### `(*device) run(ctx, out chan<- reading, interval time.Duration)`

The device loop. A `time.Ticker` at `interval`, stopped with `defer` (an unstopped
ticker leaks). Each iteration selects on:

- `<-ctx.Done()` → return.
- `<-ticker.C` → build a reading, then an **inner select** that either sends it or
  returns on cancellation.

The inner select is what makes shutdown reliable. Without it, a device blocked on a
send to a consumer that has stopped receiving would never see the cancellation, and
`wg.Wait()` would hang forever.

`out` is typed `chan<- reading` (send-only), so the compiler prevents a device from
receiving on or closing a channel it doesn't own.

**Tickers drop ticks.** If a device can't keep up, `time.Ticker` skips the missed
ticks rather than queueing them. The system produces less rather than building an
unbounded backlog, which is a crude but effective form of backpressure — and worth
knowing, because overload shows up as a lower rate rather than an error.

### `(*store) add(r reading) alarmEvent`

Called once per reading, by any worker. Holds the lock for its whole body via
`defer s.mu.Unlock()`, which is what makes the early `return raised` / `return cleared`
paths safe.

1. Look up or create the device's stats (`min: +Inf`, `max: -Inf` so the first real
   reading always wins both comparisons).
2. Update `count`, `sum`, `min`, `max`.
3. Alarm state machine:
   - Value **above** threshold: increment `aboveThreshold`. If it reaches `sustain`
     and no alarm is active, set `alarm` and return `raised`.
   - Value **at or below** threshold: reset `aboveThreshold` to 0. If an alarm was
     active, clear it and return `cleared`.
4. Otherwise return `noEvent`.

Alarms fire on the *transition*, not the state, which is why `!st.alarm` is in the
condition. Without it, every reading after the fifth would re-raise.

### `(*store) snapshot() summary`

Called by the reporter every `-window` and once more at shutdown. Under the lock, it
walks every device and:

- Counts `alarm` devices into `activeAlarms` — **before** the `count == 0` skip, so a
  device that is alarming but silent this window is still counted.
- Skips devices with no readings this window; their `min` is still `+Inf` and would
  poison the fleet minimum.
- Accumulates device count, reading count, running total, fleet min and max.
- Resets that device's window fields.

Averaging is `total / readings`, i.e. the mean per *reading*, not the mean of
per-device means — so a device reporting more often contributes proportionally more.

Reading and resetting happen in one locked section. Split into two lock/unlock pairs,
any reading arriving in between would be counted in neither window.

Map iteration order in Go is deliberately randomised; nothing here depends on it.

### `printSummary(s summary)`

Formats one window. Prints "No readings in this window" when `readings == 0`, which
avoids printing the `±Inf` sentinels.

---

## Concurrency model

### What protects what

| State | Protected by | Notes |
|---|---|---|
| `device` fields | exclusive ownership | one goroutine per device, no lock |
| `reading` in flight | the channel | copied on send |
| `store.devices` and every `deviceStats` | `store.mu` | all access via `add` / `snapshot` |
| `workerCount` | `atomic.Int64` | `Add` in workers, `Swap` in the reporter |

### Why `Swap` and not `Load` + `Store`

`Swap(0)` reads the value and zeroes it in one indivisible operation. `Load()` followed
by `Store(0)` would be two atomic operations with a gap between them, and every
increment landing in that gap would be silently lost. The race detector would not flag
it, because each call is individually atomic — the bug is in the logic, not in memory
access.

### Why printing happens outside the lock

`fmt.Printf` writes to stdout, which is slow and can block on a slow terminal. Printing
inside `add` would hold the mutex for that whole time and stall every other worker.
So `add` returns a decision, and the caller prints once the lock is released.

### Verification

```bash
go vet ./...                                    # unreachable code, lock copies, printf
go run -race . -devices=1000 -rate=10           # data races
```

The race detector reports races and exits with status 66 if it found any, which is
why CI should run tests under `-race`. The Go runtime *additionally* has a built-in
check for concurrent map access that terminates the process immediately with
`fatal error: concurrent map read and map write`; that one cannot be recovered from,
because a map interrupted mid-write can't safely be used again.

---

## Shutdown sequence

Ctrl-C triggers a chain in which no goroutine is ever killed — each one finishes on
its own terms:

1. `SIGINT` cancels `ctx` via `signal.NotifyContext`.
2. Every device's `select` takes the `<-ctx.Done()` branch and returns. Its deferred
   `ticker.Stop()` and `wg.Done()` run.
3. `wg.Wait()` in the closer returns; `close(readings)` runs.
4. Each worker's `for range readings` drains what's left, then ends. `worksWG.Done()`
   runs.
5. `worksWG.Wait()` returns; `close(done)`.
6. The reporter's `<-done` case prints the final partial-window summary and returns
   from `main`.

Because the channel is closed only after every producer has stopped, **no reading in
flight is lost** — the final summary includes the last partial window's readings.

---

## Performance characteristics

Measured on a 2-core sandbox; more cores raise these numbers:

| Configuration | Sustained |
|---|---|
| 100 devices × 10/sec | ~1,000/sec (offered load, not a limit) |
| 10,000 devices × 10/sec | ~100,000/sec |
| 10,000 devices × 100/sec | ~590,000/sec against a 1,000,000/sec target |
| 1 device × 1,000,000/sec | ~100,000/sec |

Two observations:

- **Throughput comes from many devices, not fast ones.** A single goroutine cannot
  service a 1 µs ticker, so one device asking for a million readings a second delivers
  roughly a tenth of that.
- **The plateau is the pipeline, not the devices.** Every reading crosses one
  unbuffered channel and then takes one global mutex, so those two points set the
  ceiling.

---

## Known limitations

1. **One global mutex.** Every reading from every worker serialises on `store.mu`.
   Beyond a small number of workers, more workers means more contention, not more
   throughput. Sharding the store by device ID would remove most of it.

2. **`snapshot` holds the lock while walking every device.** At 10,000 devices this is
   a brief stall for all workers, once per window, visible as a dip in the
   events/sec line.

3. **Ordering is not guaranteed.** Any worker can pick up any reading, so two readings
   from the same device can be processed out of order. "Five consecutive readings above
   threshold" is therefore approximate: the count is right, but the ordering that
   "consecutive" implies isn't enforced. Partitioning devices across workers (device ID
   modulo worker count) would give each device a single owner and make it exact.

4. **No hysteresis.** An alarm clears on the first reading at or below the threshold,
   so a device hovering near −15 °C flaps between raised and cleared. Clearing only
   below a margin (say −15.5 °C) would fix it.

5. **Unbounded state.** `store.devices` grows to one entry per device ID seen and
   nothing is ever evicted. Fine for a fixed fleet, wrong for a long-running service
   with churning IDs.

6. **Baselines are skewed warm.** `-20 + rand.Float64()*1.5` is uniform over
   −20 to −18.5, which is why the fleet average sits at exactly −19.25.
   `rand.NormFloat64()*1.5` would centre it on −20.

7. **The alarm `Printf`s are currently commented out** in the worker's `switch`, which
   leaves two empty cases. They're disabled for benchmarking; either restore them or
   remove the `switch` entirely while it's idle.

8. **Everything is in `main.go`,** and the threshold and sustain count are compile-time
   constants rather than flags.

---

## Glossary of Go constructs used

| Construct | Where | What it does |
|---|---|---|
| `go func() {...}()` | devices, workers, closer | starts a goroutine; returns immediately |
| `sync.WaitGroup` | `wg`, `worksWG` | counts outstanding goroutines |
| `chan struct{}` | `done` | a signal channel carrying no data; closing broadcasts to all receivers |
| `chan<- reading` | `run`'s parameter | send-only channel; compiler-enforced |
| `for range ch` | workers | receives until the channel is closed and drained |
| `select` | devices, reporter | waits on several channel operations at once |
| `context.Context` | cancellation | propagates "stop now" to every goroutine |
| `defer` | unlocks, `Stop()` | runs on function exit, however it exits |
| `atomic.Int64` | `workerCount` | lock-free counter |
| `sync.Mutex` | `store.mu` | mutual exclusion for the shared map |
| `iota` | `alarmEvent` | enumerates constants |
| `for i := range n` | start loops | Go 1.22+ range over an integer |
| `min` / `max` builtins | stats | Go 1.21+, no import needed |
