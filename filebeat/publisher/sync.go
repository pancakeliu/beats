package publisher

import (
	"runtime"
	"sync"

	"github.com/elastic/beats/filebeat/input"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
)

type syncLogPublisher struct {
	pub    publisher.Publisher
	client []publisher.Client
	in     chan []*input.Event
	out    SuccessLogger

	done          chan struct{}
	wg            sync.WaitGroup
	maxPublishCNT int
}

func newSyncLogPublisher(
	in chan []*input.Event,
	out SuccessLogger,
	pub publisher.Publisher,
	maxPublishCNT int,
) *syncLogPublisher {
	return &syncLogPublisher{
		in:            in,
		out:           out,
		pub:           pub,
		done:          make(chan struct{}),
		maxPublishCNT: maxPublishCNT,
	}
}

func (p *syncLogPublisher) Start() {
	MAX_PUBLISH_CNT := p.maxPublishCNT

	p.client = make([]publisher.Client, MAX_PUBLISH_CNT)

	// init connection pool
	for index := 0; index < MAX_PUBLISH_CNT; index++ {
		p.client[index] = p.pub.Connect()
	}

	p.wg.Add(MAX_PUBLISH_CNT)

	runtime.GOMAXPROCS(MAX_PUBLISH_CNT)

	for index := 0; index < MAX_PUBLISH_CNT; index++ {
		go func(index int) {
			defer p.wg.Done()

			logp.Info("Start sending events to output")
			defer logp.Debug("publisher", "Shutting down sync publisher")

			// logp.Info("index: %d", index)
			for {
				err := p.Publish(index)
				if err != nil {
					return
				}
			}
		}(index)
	}

	runtime.GOMAXPROCS(1)
}

func (p *syncLogPublisher) Publish(index int) error {
	var events []*input.Event
	select {
	case <-p.done:
		return sigPublisherStop
	case events = <-p.in:
	}

	ok := p.client[index].PublishEvents(getDataEvents(events), publisher.Sync, publisher.Guaranteed)
	if !ok {
		// PublishEvents will only returns false, if p.client has been closed.
		return sigPublisherStop
	}

	// TODO: move counter into logger?
	logp.Debug("publish", "Events sent: %d", len(events))
	eventsSent.Add(int64(len(events)))

	// Tell the logger that we've successfully sent these events
	// sync.WaitGroup.Done() all events
	ok = p.out.Published(events)
	if !ok {
		// stop publisher if successfully send events can not be logged anymore.
		return sigPublisherStop
	}
	return nil
}

func (p *syncLogPublisher) Stop() {
	MAX_PUBLISH_CNT := p.maxPublishCNT

	for index := 0; index < MAX_PUBLISH_CNT; index++ {
		p.client[index].Close()
	}

	close(p.done)
	p.wg.Wait()
}
