package memory

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Meta interface {
	GetId() string

	SetCreatedTime(time.Time)
	SetUpdatedTime(time.Time)
	SetAccessedTime(time.Time)
}

type Storage[T Meta] struct {
	data    map[string]T
	watches *watchList[T]

	lock sync.Mutex
}

func New[T Meta]() *Storage[T] {
	return &Storage[T]{
		data:    make(map[string]T),
		watches: newWatchList[T](),
	}
}

func (s *Storage[T]) Create(
	ctx context.Context,
	t T,
) (T, error) {
	log.Printf("waiting to create %T %s", t, t.GetId())

	s.lock.Lock()
	defer s.lock.Unlock()

	log.Printf("creating %T %s", t, t.GetId())

	if _, ok := s.data[t.GetId()]; ok {
		return t, status.Error(codes.AlreadyExists, "already exists")
	}

	now := time.Now()
	t.SetCreatedTime(now)
	t.SetUpdatedTime(now)
	t.SetAccessedTime(now)

	s.data[t.GetId()] = t
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

	t.SetAccessedTime(time.Now())

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
		t.SetAccessedTime(time.Now())
	}

	return ts, nil
}

func (s *Storage[T]) Update(
	ctx context.Context,
	t T,
) (T, error) {
	log.Printf("waiting to update %T %s", t, t.GetId())

	s.lock.Lock()
	defer s.lock.Unlock()

	log.Printf("updating %T %s", t, t.GetId())

	id := t.GetId()
	if _, ok := s.data[id]; !ok {
		return t, status.Error(codes.NotFound, "not found")
	}

	t.SetUpdatedTime(time.Now())
	t.SetAccessedTime(time.Now())

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
