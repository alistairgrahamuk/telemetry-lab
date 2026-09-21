package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"time"
)

type Device struct {
	ID int
}

type Reading struct {
	DeviceID int
	Value    float64
	TS       time.Time
}

func main() {

	// defaults
	devices := flag.Int("devices", 2, "Number of devices to generate readings for")
	rate := flag.Int("rate", 10, "Rate of readings per device per second")
	flag.Parse()

	if *devices < 1 || *rate < 1 || *rate > 1_000_000 {
		fmt.Fprintln(os.Stderr, "devices and rate must be >= 1, rate <= 1,000,000")
		os.Exit(2)
	}

	// Create a context that will be canceled when an interrupt signal is received Ctrl+C
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	fmt.Println("Number of devices:", *devices, "Readings per device per second:", *rate)

	fmt.Println("Creating a new devices...")

	readings := make(chan Reading)
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

	// Collector: runs in the main goroutine, collecting readings from the channel and printing counts to the console.
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	count := 0
	for {
		select {
		case _, ok := <-readings:
			if !ok {
				fmt.Println("shut down cleanly")
				return
			}
			count++
		case <-tick.C:
			fmt.Printf("%d events/sec\n", count)
			count = 0
		}
	}

}

func createDevice(id int) Device {
	return Device{
		ID: id,
	}
}

func (d *Device) run(ctx context.Context, out chan<- Reading, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop() // a ticker you don't stop is a leak
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			out <- createReading(d.ID)
		}
	}
}

func createReading(deviceID int) Reading {
	return Reading{
		DeviceID: deviceID,
		Value:    getRandomValue(),
		TS:       time.Now(),
	}
}

func getRandomValue() float64 {
	// Generate a random float64 value between 0 and 100
	return rand.Float64() * 100
}
