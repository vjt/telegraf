package parallel

import (
	"sync"

	"github.com/influxdata/telegraf"
)

type Unordered struct {
	wg      sync.WaitGroup
	acc     telegraf.Accumulator
	fn      func(telegraf.Metric) []telegraf.Metric
	inQueue chan telegraf.Metric
}

func NewUnordered(
	acc telegraf.Accumulator,
	fn func(telegraf.Metric) []telegraf.Metric,
	workerCount int,
) *Unordered {
	p := &Unordered{
		acc:     acc,
		inQueue: make(chan telegraf.Metric, workerCount),
		fn:      fn,
	}

	// start workers
	p.wg.Add(1)
	go func() {
		p.startWorkers(workerCount)
		p.wg.Done()
	}()

	return p
}

func (p *Unordered) startWorkers(count int) {
	wg := sync.WaitGroup{}
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			for metric := range p.inQueue {
				for _, m := range p.fn(metric) {
					p.acc.AddMetric(m)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (p *Unordered) Stop() {
	close(p.inQueue)
	p.wg.Wait()
}

func (p *Unordered) Enqueue(m telegraf.Metric) {
	select {
	case p.inQueue <- m:
		// Successfully queued for processing
	default:
		// Worker pool saturated — pass metric through unprocessed rather
		// than blocking the entire pipeline. This gives "best effort"
		// enrichment: metrics get processed when workers can keep up,
		// and pass through unenriched when they can't.
		p.acc.AddMetric(m)
	}
}
