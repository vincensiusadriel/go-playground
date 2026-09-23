package memcache

import (
	"container/heap"
	"errors"
	"sync"
	"time"
)

var ErrNotFound = errors.New("memcache: not found")

type expireItem struct {
	key            string
	expireUnixTime int64
	version        uint64
}

type expireHeap struct {
	items []expireItem
	index map[string]int
}

func (h *expireHeap) Len() int { return len(h.items) }
func (h *expireHeap) Less(i, j int) bool {
	return h.items[i].expireUnixTime < h.items[j].expireUnixTime
}
func (h *expireHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.index[h.items[i].key] = i
	h.index[h.items[j].key] = j
}
func (h *expireHeap) Push(x interface{}) {
	it := x.(expireItem)
	h.index[it.key] = len(h.items)
	h.items = append(h.items, it)
}
func (h *expireHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	delete(h.index, x.key)
	return x
}
func (h *expireHeap) removeKey(key string) {
	if i, ok := h.index[key]; ok {
		heap.Remove(h, i)
	}
}

type entry struct {
	value          any
	expireUnixTime int64
	version        uint64
}

type Memcache struct {
	mu        sync.Mutex
	items     map[string]entry
	hp        expireHeap
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func New() *Memcache {
	m := &Memcache{
		items: map[string]entry{},
		hp: expireHeap{
			index: map[string]int{},
		},
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go m.cleanupLoop()
	return m
}

func (m *Memcache) Close() {
	m.closeOnce.Do(func() {
		close(m.stop)
		<-m.done
	})
}

func (m *Memcache) cleanupLoop() {
	defer close(m.done)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stop:
			return
		case t := <-ticker.C:
			m.mu.Lock()
			for m.hp.Len() > 0 && m.hp.items[0].expireUnixTime <= t.UnixNano() {
				top := heap.Pop(&m.hp).(expireItem)
				curr, found := m.items[top.key]
				if !found || curr.version != top.version {
					continue
				}
				delete(m.items, top.key)
			}
			m.mu.Unlock()
		}
	}
}

func (m *Memcache) Set(key string, value any, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.hp.removeKey(key)

	var expireUnixTime int64
	if ttl > 0 {
		expireUnixTime = time.Now().Add(ttl).UnixNano()
	}

	prev := m.items[key]
	e := entry{
		value:          value,
		expireUnixTime: expireUnixTime,
		version:        prev.version + 1,
	}
	m.items[key] = e

	if expireUnixTime > 0 {
		heap.Push(&m.hp, expireItem{
			key:            key,
			expireUnixTime: expireUnixTime,
			version:        e.version,
		})
	}
}

func (m *Memcache) Get(key string) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, found := m.items[key]
	if !found {
		return nil, ErrNotFound
	}

	if e.expireUnixTime > 0 && e.expireUnixTime <= time.Now().UnixNano() {
		m.removeLocked(key)
		return nil, ErrNotFound
	}

	return e.value, nil
}

func (m *Memcache) Remove(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.removeLocked(key)
}

func (m *Memcache) removeLocked(key string) error {
	if _, found := m.items[key]; !found {
		return ErrNotFound
	}
	delete(m.items, key)
	m.hp.removeKey(key)
	return nil
}
