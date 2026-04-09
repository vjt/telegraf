package parallel_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/common/parallel"
	"github.com/influxdata/telegraf/testutil"
)

func TestOrderedJobsStayOrdered(t *testing.T) {
	acc := &testutil.Accumulator{}

	p := parallel.NewOrdered(acc, jobFunc, 10000, 10)
	now := time.Now()
	for i := 0; i < 20000; i++ {
		m := metric.New("test",
			map[string]string{},
			map[string]interface{}{
				"val": i,
			},
			now,
		)
		now = now.Add(1)
		p.Enqueue(m)
	}
	p.Stop()

	i := 0
	require.Len(t, acc.Metrics, 20000)
	for _, m := range acc.GetTelegrafMetrics() {
		v, ok := m.GetField("val")
		require.True(t, ok)
		require.EqualValues(t, i, v)
		i++
	}
}

func TestUnorderedJobsDontDropAnyJobs(t *testing.T) {
	acc := &testutil.Accumulator{}

	p := parallel.NewUnordered(acc, jobFunc, 10)

	now := time.Now()

	expectedTotal := 0
	for i := 0; i < 20000; i++ {
		expectedTotal += i
		m := metric.New("test",
			map[string]string{},
			map[string]interface{}{
				"val": i,
			},
			now,
		)
		now = now.Add(1)
		p.Enqueue(m)
	}
	p.Stop()

	actualTotal := int64(0)
	require.Len(t, acc.Metrics, 20000)
	for _, m := range acc.GetTelegrafMetrics() {
		v, ok := m.GetField("val")
		require.True(t, ok)
		actualTotal += v.(int64)
	}
	require.EqualValues(t, expectedTotal, actualTotal)
}

func BenchmarkOrdered(b *testing.B) {
	acc := &testutil.Accumulator{}

	p := parallel.NewOrdered(acc, jobFunc, 10000, 10)

	m := metric.New("test",
		map[string]string{},
		map[string]interface{}{
			"val": 1,
		},
		time.Now(),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Enqueue(m)
	}
	p.Stop()
}

func BenchmarkUnordered(b *testing.B) {
	acc := &testutil.Accumulator{}

	p := parallel.NewUnordered(acc, jobFunc, 10)

	m := metric.New("test",
		map[string]string{},
		map[string]interface{}{
			"val": 1,
		},
		time.Now(),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Enqueue(m)
	}
	p.Stop()
}

func TestUnorderedEnqueueDoesNotBlockWhenWorkersSaturated(t *testing.T) {
	acc := &testutil.Accumulator{}
	workerCount := 5

	// slowJobFunc simulates a slow processor (e.g. DNS lookup).
	// Workers hold their slots for the entire test duration.
	blocked := make(chan struct{})
	slowJobFunc := func(m telegraf.Metric) []telegraf.Metric {
		<-blocked // block forever until test cleanup
		return []telegraf.Metric{m}
	}

	p := parallel.NewUnordered(acc, slowJobFunc, workerCount)

	// Fill the channel buffer (size = workerCount) and saturate all workers.
	// Send workerCount metrics to fill the channel + workerCount to saturate workers.
	for i := 0; i < workerCount*2; i++ {
		p.Enqueue(metric.New("test",
			map[string]string{},
			map[string]interface{}{"val": i},
			time.Now(),
		))
	}

	// At this point all workers are blocked and the channel is full.
	// The next Enqueue MUST NOT block — it should pass through to the accumulator.
	done := make(chan struct{})
	go func() {
		p.Enqueue(metric.New("test",
			map[string]string{},
			map[string]interface{}{"val": 999},
			time.Now(),
		))
		close(done)
	}()

	select {
	case <-done:
		// Enqueue returned without blocking — correct behavior
	case <-time.After(1 * time.Second):
		t.Fatal("Enqueue blocked when workers were saturated — this would deadlock the pipeline")
	}

	// The passthrough metric should have been added directly to the accumulator
	require.GreaterOrEqual(t, len(acc.Metrics), 1, "passthrough metric should appear in accumulator")

	// Cleanup: unblock workers and stop
	close(blocked)
	p.Stop()
}

func jobFunc(m telegraf.Metric) []telegraf.Metric {
	return []telegraf.Metric{m}
}
