package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type device struct {
	id          int
	baseline    float64
	temperature float64
	excursion   int
}

type reading struct {
	deviceID int
	value    float64
	ts       time.Time
}

type alarmEvent int

const (
	noEvent alarmEvent = iota
	raised
	cleared
)

type deviceStats struct {
	count          int
	min, max, sum  float64
	aboveThreshold int
	alarm          bool
}

const (
	threshold = -15 // °C
	sustain   = 5   // consecutive readings above threshold before alarming
)

type store struct {
	mu      sync.Mutex
	devices map[int]*deviceStats
}

func newStore() *store {
	return &store{devices: make(map[int]*deviceStats)}
}

func main() {

	// defaults
	devices := flag.Int("devices", 2, "Number of devices to generate readings for")
	rate := flag.Int("rate", 1, "Rate of readings per device per second")
	workers := flag.Int("workers", runtime.NumCPU(), "Number of workers to process readings")
	window := flag.Duration("window", 5*time.Second, "aggregation window")
	flag.Parse()

	if *devices < 1 || *rate < 1 || *rate > 1_000_000 {
		fmt.Fprintln(os.Stderr, "devices and rate must be >= 1, rate <= 1,000,000")
		os.Exit(2)
	}

	if *workers < 1 {
		fmt.Fprintln(os.Stderr, "workers must be >= 1")
		os.Exit(2)
	}

	if *window <= 0 {
		fmt.Fprintln(os.Stderr, "window must be > 0")
		os.Exit(2)
	}

	// Create a context that will be canceled when an interrupt signal is received Ctrl+C
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	fmt.Println("Number of devices:", *devices, "Readings per device per second:", *rate)

	fmt.Println("Creating a new devices...")

	readings := make(chan reading)
	var wg sync.WaitGroup
	interval := time.Second / time.Duration(*rate)
	for i := range *devices {
		d := createDevice(i)
		wg.Add(1)
		// go keyword runs the function in a new goroutine, allowing it to run concurrently with other goroutines.
		go func() {
			defer wg.Done()
			d.run(ctx, readings, interval)
		}()
	}

	// closer
	go func() {
		wg.Wait()
		close(readings)
	}()

	// Workers
	s := newStore()
	var workerCount atomic.Int64 //  shared by every worker, deliberately unprotected.
	var worksWG sync.WaitGroup
	for range *workers {
		worksWG.Add(1)
		go func() {
			defer worksWG.Done()

			for r := range readings { // ends when the channel is closed.
				workerCount.Add(1)
				switch s.add(r) {
				case raised:
					// fmt.Printf("ALARM: device %d above threshold %.2f at %s\n", r.deviceID, r.value, r.ts.Format(time.RFC3339))
					// does not require break as case statements in Go do not fall through by default
				case cleared:
					// fmt.Printf("CLEARED: device %d below threshold %.2f at %s\n", r.deviceID, r.value, r.ts.Format(time.RFC3339))
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		worksWG.Wait()
		close(done)
	}()

	// Reporter
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	// ticker aggregation
	windowTicker := time.NewTicker(*window)
	defer windowTicker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Printf("%d events/sec\n", workerCount.Swap(0))
		case <-windowTicker.C:
			printSummary(s.snapshot())
		case <-done:
			printSummary(s.snapshot()) // final, partial window
			fmt.Println("Shut down cleanly")
			return
		}
	}

}

func printSummary(s summary) {
	fmt.Println("--------------------------------------------------")
	fmt.Printf("Devices: %d, Readings: %d, Active Alarms: %d\n", s.devices, s.readings, s.activeAlarms)
	if s.readings > 0 {
		fmt.Printf("Min: %.2f, Max: %.2f, Avg: %.2f\n", s.min, s.max, s.avg)
	} else {
		fmt.Println("No readings in this window")
	}
}

func createDevice(id int) device {
	b := -20 + rand.Float64()*1.5
	return device{id: id, baseline: b, temperature: b}
}

func (d *device) next() float64 {

	if d.excursion == 0 && rand.Float64() < 0.00005 {
		d.excursion = 40
	}

	if d.excursion > 0 {
		d.temperature += 0.3
		d.excursion--

	}

	noise := rand.NormFloat64() * 0.2           // small Gaussian wobble
	pull := (d.baseline - d.temperature) * 0.05 // drift back towards baseline
	d.temperature += noise + pull
	return d.temperature
}

func (d *device) run(ctx context.Context, out chan<- reading, interval time.Duration) {
	ticker := time.NewTicker(interval)

	defer ticker.Stop() // a ticker you don't stop is a leak
	for {
		select {
		case <-ticker.C:
			r := reading{deviceID: d.id, value: d.next(), ts: time.Now()}
			// fmt.Printf("%.2f\n", r.value)
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

type summary struct {
	devices, readings, activeAlarms int
	min, max, avg                   float64
}

func (s *store) add(r reading) alarmEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	st, ok := s.devices[r.deviceID]
	if !ok {
		st = &deviceStats{min: math.Inf(1), max: math.Inf(-1)}
		s.devices[r.deviceID] = st
	}

	// window stats
	st.count++
	st.sum += r.value
	st.min = min(st.min, r.value)
	st.max = max(st.max, r.value)

	// alarm state
	if r.value > threshold {
		st.aboveThreshold++
		if st.aboveThreshold >= sustain && !st.alarm {
			st.alarm = true
			return raised
		}
	} else {
		st.aboveThreshold = 0
		if st.alarm {
			st.alarm = false
			return cleared
		}
	}
	return noEvent
}

func (s *store) snapshot() summary {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := summary{min: math.Inf(1), max: math.Inf(-1)}
	var total float64

	for _, st := range s.devices {
		if st.alarm {
			out.activeAlarms++ // counted even if the device was silent this window
		}
		if st.count == 0 {
			continue // no readings this window: its min is +Inf, so skip it
		}
		out.devices++
		out.readings += st.count
		total += st.sum
		out.min = min(out.min, st.min)
		out.max = max(out.max, st.max)

		// reset the window fields; alarm state carries over
		st.count, st.sum = 0, 0
		st.min, st.max = math.Inf(1), math.Inf(-1)
	}

	if out.readings > 0 {
		out.avg = total / float64(out.readings)
	}
	return out
}
