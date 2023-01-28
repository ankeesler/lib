package memory

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"runtime"
	"sync"
)

type watchContext[T Meta] struct {
	id  *string
	ctx context.Context
	c   chan T

	source string
}

type watchList[T Meta] struct {
	m    map[string]*watchContext[T]
	lock sync.Mutex
}

func newWatchList[T Meta]() *watchList[T] {
	return &watchList[T]{
		m: make(map[string]*watchContext[T]),
	}
}

func (l *watchList[T]) add(ctx context.Context, id *string) chan T {
	_, callerFile, callerLine, ok := runtime.Caller(2)
	if !ok {
		callerFile = "???"
		callerLine = 0
	} else {
		callerFile = filepath.Base(callerFile)
	}

	var t T
	log.Printf("waiting to open watch for %T and id %+v from %s:%d",
		t, id, callerFile, callerLine)

	l.lock.Lock()
	defer l.lock.Unlock()

	log.Printf("opening watch for %T and id %+v from %s:%d",
		t, id, callerFile, callerLine)

	wc := &watchContext[T]{
		ctx: ctx,
		id:  id,
		c:   make(chan T, 1),

		source: fmt.Sprintf("%s:%d", callerFile, callerLine),
	}
	l.m[fmt.Sprintf("%x", rand.Int63())] = wc

	return wc.c
}

func (l *watchList[T]) notify(t T) {
	log.Printf("kicking watch for %T %s", t, t.ID())

	l.lock.Lock()
	defer l.lock.Unlock()

	var toDelete []string
	for id, watchCtx := range l.m {
		if watchCtx.id == nil || *watchCtx.id == t.ID() {
			log.Printf("about to watch id %+v from %s", t.ID(), watchCtx.source)
			select {
			case <-watchCtx.ctx.Done():
				log.Printf("watched was closed for id %+v from %s", t.ID(), watchCtx.source)
				toDelete = append(toDelete, id)
			case watchCtx.c <- t:
				log.Printf("watched id %+v from %s", t.ID(), watchCtx.source)
			}
		}
	}
	for _, toDeleteID := range toDelete {
		delete(l.m, toDeleteID)
	}
}
