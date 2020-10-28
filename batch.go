package batch

import (
	"context"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

const MaxParallelGets = 16

type result struct {
	b   blocks.Block
	err error
}

type request struct {
	retCh chan *result
	key   cid.Cid
}

type BatchGet struct {
	getCh chan *request
}

func (b *BatchGet) Start(ctx context.Context) {
	b.getCh = make(chan *request)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			// Shut down on cancelation
			select {
			case <-ctx.Done():
				break
			default:
			}

			// Gather MaxParallelGets requests
			var reqBuf []*request
		BUFFER_LOOP:
			for {
				select {
				case req := <-b.getCh:
					reqBuf = append(reqBuf, req)
				case <-ctx.Done():
					break BUFFER_LOOP
				case <-ticker.C:
				}

				// Flush
				if len(reqBuf) > 0 {
					// TODO, gather keys and do badger multiple reads in one tx
					results := make([]*result, len(reqBuf))
					// Return results to getting goroutines
					for i, req := range reqBuf {
						req.retCh <- results[i]
					}
					break
				}
			}
		}
	}()
}

func (b *BatchGet) Get(c cid.Cid) (blocks.Block, error) {
	retCh := make(chan *result)
	req := &request{
		key:   c,
		retCh: retCh,
	}
	b.getCh <- req
	result, ok := <-retCh
	if !ok {
		return nil, xerrors.Errorf("early return channel close")
	}
	return result.b, result.err
}
