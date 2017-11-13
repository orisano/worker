package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

type Group interface {
	Spawn(fn func(ctx context.Context)) int32
	Stop(id int32) bool
	StopAll()
	Wait()
	Size() int
}

func NewGroup(ctx context.Context) (Group, error) {
	if ctx == nil {
		return nil, errors.New("missing ctx")
	}
	ctx, cancel := context.WithCancel(ctx)
	return &group{
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

type group struct {
	ctx    context.Context
	cancel context.CancelFunc

	id        int32
	wg        sync.WaitGroup
	running   map[int32]context.CancelFunc
	runningMu sync.RWMutex
}

func (g *group) Spawn(fn func(ctx context.Context)) int32 {
	ctx, cancel := context.WithCancel(g.ctx)
	id := atomic.AddInt32(&g.id, 1)
	g.runningMu.Lock()
	g.running[id] = cancel
	g.runningMu.Unlock()
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		defer func() {
			g.runningMu.Lock()
			delete(g.running, id)
			g.runningMu.Unlock()
		}()
		defer cancel()
		fn(ctx)
	}()
	return id
}

func (g *group) Stop(id int32) bool {
	g.runningMu.Lock()
	cancel, ok := g.running[id]
	if ok {
		cancel()
		delete(g.running, id)
	}
	g.runningMu.Unlock()
	return ok
}

func (g *group) StopAll() {
	g.cancel()
}

func (g *group) Wait() {
	g.wg.Wait()
}

func (g *group) Size() int {
	g.runningMu.RLock()
	defer g.runningMu.RUnlock()
	return len(g.running)
}
