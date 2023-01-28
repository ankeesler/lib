package memory

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Meta interface {
	GetId() string
	SetId(string)

	GetCreatedTime() time.Time
	SetCreatedTime(time.Time)

	GetUpdatedTime() time.Time
	SetUpdatedTime(time.Time)
}

type Uint64er interface {
	Uint64() uint64
}

type Storage[T Meta] struct {
	r Uint64er

	data    map[string]T
	watches *watchList[T]

	lock sync.Mutex
}

func New[T Meta](r Uint64er) *Storage[T] {
	return &Storage[T]{
		r: r,

		data:    make(map[string]T),
		watches: newWatchList[T](),
	}
}

func (s *Storage[T]) Create(
	ctx context.Context,
	t T,
) (T, error) {
	log.Printf("waiting to create %T %s", t, t.ID())

	s.lock.Lock()
	defer s.lock.Unlock()

	log.Printf("creating %T %s", t, t.ID())

	id := fmt.Sprintf("%x", s.r.Uint64())
	if _, ok := s.data[id]; ok {
		return t, status.Error(codes.AlreadyExists, "already exists")
	}

	t.SetID(id)
	t.SetCreatedTime(time.Now())
	t.SetUpdatedTime(t.CreatedTime())

	s.data[t.ID()] = t
	go s.watches.notify(t)

	return t, nil
}

func (s *Storage[T]) Get(
	ctx context.Context,
	id string,
) (T, error) {
	var t T
	log.Printf("waiting to get %T %s", t, id)

	s.lock.Lock()
	defer s.lock.Unlock()

	log.Printf("getting %T %s", t, id)

	var ok bool
	t, ok = s.data[id]
	if !ok {
		return t, status.Error(codes.NotFound, "not found")
	}

	return t, nil
}

func (s *Storage[T]) Watch(
	ctx context.Context,
	id *string,
) (<-chan T, error) {
	var t T
	log.Printf("waiting to watch %T %+v", t, id)

	s.lock.Lock()
	defer s.lock.Unlock()

	log.Printf("watching %T %+v", t, id)

	c := s.watches.add(ctx, id)

	if id != nil {
		if t, ok := s.data[*id]; ok {
			c <- t
		}
	}

	return c, nil
}

func (s *Storage[T]) List(
	ctx context.Context,
) ([]T, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	log.Print("listing")

	var ts []T
	for _, t := range s.data {
		ts = append(ts, t)
	}

	return ts, nil
}

func (s *Storage[T]) Update(
	ctx context.Context,
	t T,
) (T, error) {
	log.Printf("waiting to update %T %s", t, t.ID())

	s.lock.Lock()
	defer s.lock.Unlock()

	log.Printf("updating %T %s", t, t.ID())

	id := t.ID()
	if _, ok := s.data[id]; !ok {
		return t, status.Error(codes.NotFound, "not found")
	}

	t.SetUpdatedTime(time.Now())

	s.data[id] = t
	go s.watches.notify(t)

	return t, nil
}

func (s *Storage[T]) Delete(
	ctx context.Context,
	id string,
) (T, error) {
	var t T
	log.Printf("waiting to delete %T %s", t, id)

	s.lock.Lock()
	defer s.lock.Unlock()

	log.Printf("deleting %T %s", t, id)

	var ok bool
	t, ok = s.data[id]
	if !ok {
		return t, status.Error(codes.NotFound, "not found")
	}

	delete(s.data, id)

	return t, nil
}
